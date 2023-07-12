package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var conn *pgx.Conn

func main() {
	var err error
	conn, err = pgx.Connect(context.Background(), "postgresql://ledger_gateway:ledger_gateway@localhost:5432/ledger")
	// pgx.ConnectConfig(context.Background(), &pgx.ConnConfig{})
	if err != nil {
		log.Fatalln(err)
	}
	defer conn.Close(context.Background())

	{ // TEST CODE
		source := WalletId(uuid.New())
		dest := WalletId(uuid.New())
		log.Println(source.String())
		amount := int64(100)
		_, err := conn.Exec(context.Background(), "insert into wallets (address, negative_limit) values ($1, 50), ($2, 0)", source, dest)
		if err != nil {
			log.Fatal(err)
		}

		_, err = conn.Exec(context.Background(), "insert into accounts (balance, wallet_id) values($1, $2)", amount-50, source)
		if err != nil {
			log.Fatal(err)
		}

		txs := []Transaction{
			{source, dest, NewBigInt(amount), nil},
			{dest, source, NewBigInt(amount / 2), nil},
		}
		ids, err := TransactBatch(txs...)
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Println(ids)

		r_ids, err := BatchRevert(ids[1], ids[0])
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Println(r_ids)

		// tx_id, err := Transact(source, dest, amount)
		// if err != nil {
		// 	log.Fatalln(err)
		// }
		// revert_id, err := RevertTransaction(tx_id)
		// if err != nil {
		// 	log.Fatalln(err)
		// }

		// _, err = RevertTransaction(tx_id)
		// if err != nil {
		// 	log.Println(err)
		// }
		// _, err = RevertTransaction(revert_id)
		// if err != nil {
		// 	log.Println(err)
		// }
	}

}

type Transaction struct {
	Source      WalletId
	Destination WalletId
	Amount      *BigInt
	// Amount   int /* TODO: what data type should we use for precision? WARN*/
	Metadata map[string]interface{}
}

type Metadata map[string]interface{}

func TransactBatch(txs ...Transaction) ([]int, error) {
	if len(txs) == 0 {
		return nil, errors.New("empty array of transactions")
	}

	wallets_balances := make(map[WalletId]*BigInt)
	for _, tx := range txs {
		wallets_balances[tx.Source] = NewBigInt(0)
		wallets_balances[tx.Destination] = NewBigInt(0)
		if tx.Amount.Cmp(ZERO) == 0 {
			return nil, errors.New("invalid amount")
		}
	}

	wallet_ids := make([]WalletId, len(wallets_balances))
	i := 0
	for wallet_id := range wallets_balances {
		wallet_ids[i] = wallet_id
		i += 1
		if !CheckWalletExists(wallet_id) {
			return nil, errors.New("invalid wallet id")
		}
	}

	db_tx, err := conn.Begin(context.Background())
	if err != nil {
		return nil, err
	}
	defer db_tx.Rollback(context.Background())
	err = LockWallets(db_tx, wallet_ids...)
	if err != nil {
		return nil, err
	}

	{
		sql := "select w.address, sum(a.balance) + w.negative_limit from wallets w" +
			" join accounts a on a.wallet_id = w.address " +
			" where w.address = any ($1) group by w.address"
		err = RunQuery(db_tx, func(row pgx.CollectableRow) error {
			var wallet_id WalletId
			balance := NewBigInt(0)
			err := row.Scan(&wallet_id, &balance)
			wallets_balances[WalletId(wallet_id)].Add(balance)
			return err
		}, sql, wallet_ids)
		if err != nil {
			return nil, err
		}
	}

	for _, tx := range txs {
		wallets_balances[tx.Source].Sub(tx.Amount)
		wallets_balances[tx.Destination].Add(tx.Amount)
	}

	for _, balance := range wallets_balances {
		if balance.Cmp(ZERO) <= 0 {
			return nil, errors.New("insufficient funds")
		}
	}

	type Account struct {
		Address AccountId
		Balance *BigInt
	}

	wallets := make(map[WalletId][]Account)
	{
		select_accounts_sql := "select address, balance, wallet_id from accounts where wallet_id = any ($1) order by created asc"
		err = RunQuery(db_tx, func(row pgx.CollectableRow) error {
			var account Account
			var wallet_id uuid.UUID
			err := row.Scan(&account.Address, &account.Balance, &wallet_id)
			wallets[WalletId(wallet_id)] = append(wallets[WalletId(wallet_id)], account)
			return err
		}, select_accounts_sql, wallet_ids)
		if err != nil {
			return nil, err
		}
	}

	var postings []PostingInsert
	type AccountInsert struct {
		WalletId WalletId
		Address  AccountId
		Amount   *BigInt
	}
	var account_inserts []AccountInsert

	account_updates := make(map[AccountId]*BigInt)

	for tx_index, tx := range txs {
		dest_acc_id := AccountId(uuid.New())
		account_inserts = append(account_inserts, AccountInsert{tx.Destination, dest_acc_id, CopyBigInt(tx.Amount)})
		wallets[tx.Destination] = append(wallets[tx.Destination], Account{dest_acc_id, CopyBigInt(tx.Amount)})

		remaining := CopyBigInt(tx.Amount)
		for i, account := range wallets[tx.Source] {
			if account.Balance.Cmp(ZERO) <= 0 {
				continue
			}
			posting_amount := Min(account.Balance, tx.Amount)
			postings = append(postings, PostingInsert{account.Address, dest_acc_id, CopyBigInt(posting_amount), tx_index})
			remaining.Sub(posting_amount)
			wallets[tx.Source][i].Balance.Sub(posting_amount) // WARN: for some reason, this alters posting_amount. WHY?
			if account_updates[account.Address] == nil {
				account_updates[account.Address] = NewBigInt(0)
			}
			account_updates[account.Address].Add(wallets[tx.Source][i].Balance)
			if remaining.Cmp(ZERO) == 0 {
				break
			}
		}

		if remaining.Cmp(ZERO) == 1 {
			// WARN: this assumes the total balance > -negative_limit
			credit_acc_id := AccountId(uuid.New())
			neg_remaining := CopyBigInt(remaining)
			neg_remaining.Neg()
			account_inserts = append(account_inserts, AccountInsert{tx.Source, credit_acc_id, neg_remaining})
			wallets[tx.Source] = append(wallets[tx.Source], Account{credit_acc_id, CopyBigInt(neg_remaining)})
			postings = append(postings, PostingInsert{credit_acc_id, dest_acc_id, CopyBigInt(remaining), tx_index})
		}
	}

	{ // Batch insert accounts
		args := make([]any, len(account_inserts)*3)
		b := strings.Builder{}
		for i := range account_inserts {
			b.WriteString(fmt.Sprintf("($%d, $%d, $%d),", i*3+1, i*3+2, i*3+3))
			acc := account_inserts[i]
			args[i*3] = acc.WalletId
			args[i*3+1] = acc.Address
			args[i*3+2] = acc.Amount.String()
		}
		str := b.String()
		sql := fmt.Sprintf("insert into accounts (wallet_id, address, balance) values %s", str[:len(str)-1])
		_, err := conn.Exec(context.Background(), sql, args...)
		if err != nil {
			return nil, err
		}
	}
	tx_ids, err := transact(db_tx, account_updates, txs, postings)
	if err != nil {
		return nil, err
	}

	err = db_tx.Commit(context.Background())
	if err != nil {
		return nil, err
	}

	return tx_ids, nil
}

func BatchRevert(tx_ids ...int) ([]int, error) {
	if len(tx_ids) == 0 {
		return nil, errors.New("empty array")
	}

	{
		// Remove unique ids from tx_ids
		unique_ids := make(map[int]bool)
		list := []int{}
		for _, id := range tx_ids {
			if id <= 0 {
				return nil, errors.New("invalid transaction id")
			}
			if _, value := unique_ids[id]; !value {
				unique_ids[id] = true
				list = append(list, id)
			}
		}
		tx_ids = list
	}

	var wallet_ids []WalletId
	{
		var err error
		sql := "select distinct unnest(array[source, destination]) from transactions where id = any ($1)"
		wallet_ids, err = QueryCollect(conn, func(row pgx.CollectableRow) (WalletId, error) {
			var wallet_id WalletId
			err := row.Scan(&wallet_id)
			return wallet_id, err
		}, sql, tx_ids)
		if err != nil {
			return nil, err
		}
	}

	db_tx, err := conn.Begin(context.Background())
	if err != nil {
		return nil, err
	}
	defer db_tx.Rollback(context.Background())

	err = LockWallets(db_tx, wallet_ids...)
	if err != nil {
		return nil, err
	}

	tx_index_map := make(map[int]int)
	var txs []Transaction
	{
		sql := "select id, source, destination, amount, metadata from transactions where id = any($1) order by id asc"
		i := 0
		txs, err = QueryCollect(db_tx, func(row pgx.CollectableRow) (Transaction, error) {
			var tx Transaction
			var id int
			var metadata Metadata
			revert_metadata := make(Metadata)
			err = row.Scan(&id, &tx.Destination, &tx.Source, &tx.Amount, &metadata) // NOTE: inverted destination and source

			if metadata["reverted_by"] != nil {
				return tx, errors.New("transaction already reverted")
			}
			if metadata["reverts"] != nil {
				return tx, errors.New("transaction reverts another one")
			}

			revert_metadata["reverts"] = id
			tx.Metadata = revert_metadata
			tx_index_map[id] = i
			i++
			return tx, err
		}, sql, tx_ids)
		if err != nil {
			return nil, err
		}
	}

	type Posting struct {
		Source AccountId
		Dest   AccountId
		Amount *BigInt
		TxId   int
	}

	var account_ids []AccountId
	var postings []Posting
	{
		sql := "select p.source, p.destination, p.amount, p.transaction_id from postings p " +
			"join transactions t on t.id = p.transaction_id where p.transaction_id = any ($1) "
		postings, err = QueryCollect(db_tx, func(row pgx.CollectableRow) (Posting, error) {
			var posting Posting
			// NOTE: inverted source and destination
			err = row.Scan(&posting.Dest, &posting.Source, &posting.Amount, &posting.TxId)
			account_ids = append(account_ids, posting.Source, posting.Dest)
			return posting, err
		}, sql, tx_ids)
		if err != nil {
			return nil, err
		}
	}

	accounts := make(map[AccountId]*BigInt)
	{
		sql := "select address, balance from accounts a where address = any($1)"
		err = RunQuery(db_tx, func(row pgx.CollectableRow) error {
			var account_id AccountId
			balance := NewBigInt(0)
			err = row.Scan(&account_id, &balance)
			accounts[account_id] = balance
			return err
		}, sql, account_ids)
		if err != nil {
			return nil, err
		}
	}

	postings_insert := make([]PostingInsert, len(postings))
	for i, posting := range postings {
		accounts[posting.Source].Sub(posting.Amount)
		accounts[posting.Dest].Add(posting.Amount)
		postings_insert[i] = PostingInsert{posting.Source, posting.Dest, CopyBigInt(posting.Amount), tx_index_map[posting.TxId]}
	}
	for _, balance := range accounts {
		if balance.Cmp(ZERO) == -1 {
			return nil, errors.New("unable to revert")
		}
	}

	revert_tx_ids, err := transact(db_tx, accounts, txs, postings_insert)
	if err != nil {
		return nil, err
	}

	{ // add reverted_by metadata to transactions
		sort.Ints(tx_ids) // WARN: this brakes if there are repeated tx_ids
		sort.Ints(revert_tx_ids)
		update_acc_sql := "update transactions t set metadata = jsonb_set(metadata, '{reverted_by}', to_jsonb(nt.value))" +
			" from (select unnest($1::numeric[]) as key, unnest($2::numeric[]) as value) as nt" +
			" where t.id = nt.key"
		_, err := db_tx.Exec(context.Background(), update_acc_sql, tx_ids, revert_tx_ids)
		if err != nil {
			return nil, err
		}
	}

	err = db_tx.Commit(context.Background())
	if err != nil {
		return nil, err
	}

	return revert_tx_ids, nil
}

type PostingInsert struct {
	Source      AccountId
	Destination AccountId
	Amount      *BigInt
	TxIdIndex   int
}

func transact(db pgx.Tx, account_updates map[AccountId]*BigInt, txs []Transaction, postings []PostingInsert) ([]int, error) {
	{
		acc_update_ids := make([]AccountId, len(account_updates))
		acc_update_amounts := make([]string, len(account_updates))

		i := 0
		for id, amount := range account_updates {
			acc_update_ids[i] = id
			acc_update_amounts[i] = amount.String()
			i += 1
		}

		update_acc_sql := "update accounts a set balance = na.value" +
			" from (select unnest($1::uuid[]) as key, unnest($2::numeric[]) as value) as na" +
			" where a.address = na.key"
		_, err := db.Exec(context.Background(), update_acc_sql, acc_update_ids, acc_update_amounts)
		if err != nil {
			return nil, err
		}
	}

	empty_metadata := make(Metadata)
	tx_ids := make([]int, len(txs))
	{
		args := make([]any, len(txs)*4)
		b := strings.Builder{}
		for i := range txs {
			b.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d),", i*4+1, i*4+2, i*4+3, i*4+4))
			args[i*4] = txs[i].Source
			args[i*4+1] = txs[i].Destination
			args[i*4+2] = txs[i].Amount.String()
			if txs[i].Metadata == nil {
				args[i*4+3] = empty_metadata
			} else {
				args[i*4+3] = txs[i].Metadata
			}
		}
		str := b.String()
		str = str[:len(str)-1]
		sql := fmt.Sprintf("insert into transactions (source, destination, amount, metadata) values %s returning id", str)
		j := -1
		err := RunQuery(db, func(row pgx.CollectableRow) error {
			j++
			return row.Scan(&tx_ids[j])
		}, sql, args...)
		if err != nil {
			return nil, err
		}
	}

	{
		args := make([]any, len(postings)*4)
		b := strings.Builder{}
		for i, posting := range postings {
			b.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d),", i*4+1, i*4+2, i*4+3, i*4+4))
			args[i*4] = tx_ids[posting.TxIdIndex]
			args[i*4+1] = posting.Source
			args[i*4+2] = posting.Destination
			args[i*4+3] = posting.Amount.String()
		}
		str := b.String()
		str = str[:len(str)-1]
		insert_sql := fmt.Sprintf("insert into postings (transaction_id, source, destination, amount) values %s", str)
		_, err := db.Exec(context.Background(), insert_sql, args...)
		if err != nil {
			return nil, err
		}
	}

	return tx_ids, nil
}

type Db interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func QueryCollect[T any](db Db, rowToFunc pgx.RowToFunc[T], query string, args ...any) ([]T, error) {
	rows, err := db.Query(context.Background(), query, args...)
	if err != nil {
		return nil, err
	}
	t, err := pgx.CollectRows(rows, rowToFunc)
	return t, err
}

func RunQuery(db Db, fn func(row pgx.CollectableRow) error, query string, args ...any) error {
	rows, err := db.Query(context.Background(), query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		err := fn(rows)
		if err != nil {
			return err
		}
	}
	return rows.Err()
}

func LockWallets(db pgx.Tx, wallets ...WalletId) error {
	// WARN: this doesnt prevent accounts from theese wallets to be altered or created. This is here to prevent this function to run concurrently on the same wallets
	// NOTE: copied the idea from https://stackoverflow.com/a/52557413/13449544
	// NOTE: this doensn't lock from read... TODO: should we lock from read?
	if len(wallets) == 0 {
		return nil
	}
	_, err := db.Exec(context.Background(), "lock table wallets in row exclusive mode")
	if err != nil {
		return err
	}
	sql := "select * from wallets where address = any ($1) for update"
	_, err = db.Exec(context.Background(), sql, wallets)
	return err
}

func CheckWalletExists(wallet_id WalletId) bool {
	exists := false
	wallet_select := "select exists (select address from wallets where address = $1)"
	source_wallet_row := conn.QueryRow(context.Background(), wallet_select, wallet_id.String())
	err := source_wallet_row.Scan(&exists)
	if err != nil {
		return false
	}
	return exists
}

func Min(a, b *BigInt) *BigInt {
	x := NewBigInt(0)
	if a.Cmp(b) == -1 {
		x.Set(a)
		return a
	}
	x.Set(b)
	return b
}
