package main

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"log"
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
		amount := 100
		_, err := conn.Exec(context.Background(), "insert into wallets (address, negative_limit) values ($1, 50), ($2, 0)", source, dest)
		if err != nil {
			log.Fatal(err)
		}

		_, err = conn.Exec(context.Background(), "insert into accounts (balance, wallet_id) values($1, $2)", amount-50, source)
		if err != nil {
			log.Fatal(err)
		}

		txs := []Transaction{
			{source, dest, amount},
			{dest, source, amount / 2},
		}
		ids, err := TransactBatch(txs...)
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Println(ids)

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

type AccountId uuid.UUID
type WalletId uuid.UUID

func (id WalletId) String() string {
	return uuid.UUID(id).String()
}
func (id AccountId) String() string {
	return uuid.UUID(id).String()
}

func (id WalletId) Value() (driver.Value, error) {
	return uuid.UUID(id).Value()
}
func (id AccountId) Value() (driver.Value, error) {
	return uuid.UUID(id).Value()
}

func (id *WalletId) Scan(src any) error {
	new_id, err := uuid.Parse(src.(string))
	*id = WalletId(new_id)
	return err
}
func (id *AccountId) Scan(src any) error {
	new_id, err := uuid.Parse(src.(string))
	*id = AccountId(new_id)
	return err
}

type Transaction struct {
	Source      WalletId
	Destination WalletId
	Amount      int /* TODO: what data type should we use for precision? WARN*/
}

func TransactBatch(txs ...Transaction) ([]int, error) {
	if len(txs) == 0 {
		return nil, errors.New("empty array of transactions")
	}

	wallets_balances := make(map[WalletId]int)
	for _, tx := range txs {
		wallets_balances[tx.Source] = 0
		wallets_balances[tx.Destination] = 0
		if tx.Amount == 0 {
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
		err = QueryCollect(db_tx, func(row pgx.CollectableRow) error {
			var wallet_id WalletId
			var balance int
			err := row.Scan(&wallet_id, &balance)
			wallets_balances[WalletId(wallet_id)] += balance
			return err
		}, sql, wallet_ids)
		if err != nil {
			return nil, err
		}
	}

	for _, tx := range txs {
		wallets_balances[tx.Source] -= tx.Amount
		wallets_balances[tx.Destination] += tx.Amount
	}

	for _, balance := range wallets_balances {
		if balance <= 0 {
			return nil, errors.New("insufficient funds")
		}
	}

	type Account struct {
		Address AccountId
		Balance int
	}

	wallets := make(map[WalletId][]Account)
	{
		select_accounts_sql := "select address, balance, wallet_id from accounts where wallet_id = any ($1) order by created asc"
		err = QueryCollect(db_tx, func(row pgx.CollectableRow) error {
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

	type Posting struct {
		Source      AccountId
		Destination AccountId
		Amount      int
		TxIdIndex   int
	}
	var postings []Posting
	type AccountInsert struct {
		WalletId WalletId
		Address  AccountId
		Amount   int
	}
	var account_inserts []AccountInsert

	account_updates := make(map[AccountId]int)

	for tx_index, tx := range txs {
		dest_acc_id := AccountId(uuid.New())
		account_inserts = append(account_inserts, AccountInsert{tx.Destination, dest_acc_id, tx.Amount})
		wallets[tx.Destination] = append(wallets[tx.Destination], Account{dest_acc_id, tx.Amount})

		remaining := tx.Amount
		for i, account := range wallets[tx.Source] {
			if account.Balance <= 0 {
				continue
			}
			posting_amount := Min(account.Balance, tx.Amount)
			postings = append(postings, Posting{account.Address, dest_acc_id, posting_amount, tx_index})
			remaining -= posting_amount
			wallets[tx.Source][i].Balance -= posting_amount
			account_updates[account.Address] += posting_amount
			if remaining == 0 {
				break
			}
		}

		if remaining > 0 {
			// WARN: this assumes the total balance > -negative_limit
			credit_acc_id := AccountId(uuid.New())
			account_inserts = append(account_inserts, AccountInsert{tx.Source, credit_acc_id, -remaining})
			wallets[tx.Source] = append(wallets[tx.Source], Account{credit_acc_id, -remaining})
			postings = append(postings, Posting{credit_acc_id, dest_acc_id, remaining, tx_index})
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
			args[i*3+2] = acc.Amount
		}
		str := b.String()
		sql := fmt.Sprintf("insert into accounts (wallet_id, address, balance) values %s", str[:len(str)-1])
		_, err := conn.Exec(context.Background(), sql, args...)
		if err != nil {
			return nil, err
		}
	}

	{ // Batch update source accounts
		acc_update_ids := make([]AccountId, len(account_updates))
		acc_update_amounts := make([]int, len(account_updates))

		i := 0
		for id, amount := range account_updates {
			acc_update_ids[i] = id
			acc_update_amounts[i] = amount
			i += 1
		}

		update_acc_sql := "update accounts a set balance = balance - na.value" +
			" from (select unnest($1::uuid[]) as key, unnest($2::numeric[]) as value) as na" +
			" where a.address = na.key"
		_, err = conn.Exec(context.Background(), update_acc_sql, acc_update_ids, acc_update_amounts)
		if err != nil {
			log.Fatalln(err)
		}
	}

	tx_ids := make([]int, len(txs))

	{ // insert transactions
		args := make([]any, len(txs)*3)
		b := strings.Builder{}
		for i := range txs {
			b.WriteString(fmt.Sprintf("($%d, $%d, $%d),", i*3+1, i*3+2, i*3+3))
			args[i*3] = txs[i].Source
			args[i*3+1] = txs[i].Destination
			args[i*3+2] = txs[i].Amount
		}
		str := b.String()
		str = str[:len(str)-1]
		sql := fmt.Sprintf("insert into transactions (source, destination, amount) values %s returning id", str)
		j := -1
		err = QueryCollect(db_tx, func(row pgx.CollectableRow) error {
			j++
			return row.Scan(&tx_ids[j])
		}, sql, args...)
		if err != nil {
			return nil, err
		}
	}

	for _, posting := range postings { // TODO: batch insert
		insert_sql := "insert into postings (transaction_id, source, destination, amount) values ($1, $2, $3, $4)"
		_, err = db_tx.Exec(context.Background(), insert_sql, tx_ids[posting.TxIdIndex],
			posting.Source, posting.Destination, posting.Amount)
		if err != nil {
			return nil, err
		}
	}

	err = db_tx.Commit(context.Background())
	if err != nil {
		return nil, err
	}

	return tx_ids, nil
}

func Transact(source, destination WalletId, amount int) (int, error) {
	tx, err := conn.Begin(context.Background())
	if err != nil {
		return -1, err
	}
	defer tx.Rollback(context.Background())
	tx_id, err := TransactWithTransaction(source, destination, amount, tx)
	if err != nil {
		return -1, err
	}
	err = tx.Commit(context.Background())
	if err != nil {
		return -1, err
	}
	return tx_id, nil
}

func TransactWithTransaction(source, destination WalletId, amount int, ongoing_tx pgx.Tx) (int, error) {
	if amount <= 0 {
		return -1, errors.New("invalid amount")
	}

	if !CheckWalletExists(source) {
		return -1, errors.New("source wallet doesn't exist")
	}
	if !CheckWalletExists(destination) {
		return -1, errors.New("destination wallet doesn't exist")
	}

	var total_source_balance int
	{
		total_balance_sql := "select sum(a.balance) + w.negative_limit from wallets w " +
			"join accounts a on a.wallet_id = w.address " +
			"where w.address = $1 group by w.address"
		row := conn.QueryRow(context.Background(), total_balance_sql, source.String())
		err := row.Scan(&total_source_balance)
		if err != nil {
			return -1, err
		}
	}

	if total_source_balance < amount {
		return -1, errors.New("insufficient funds")
	}

	// ongoing_tx, err := conn.Begin(context.Background())
	// if err != nil {
	// 	return -1, err
	// }
	// defer ongoing_tx.Rollback(context.Background()) // TODO: ignore the error?

	// TODO: lock accounts? How can I prevent an account to be created with wallet_id in (source, destination) during this transaction?
	err := LockWallets(ongoing_tx, source, destination)
	if err != nil {
		return -1, err
	}

	type Account struct {
		Address uuid.UUID
		Balance int
	}
	var accounts []Account
	{
		select_accounts_sql := "select address, balance from accounts where wallet_id = $1 order by created asc"
		rows, err := ongoing_tx.Query(context.Background(), select_accounts_sql, source.String())
		if err != nil {
			return -1, err
		}
		defer rows.Close()
		for rows.Next() {
			var account Account
			err = rows.Scan(&account.Address, &account.Balance)
			if err != nil {
				return -1, err
			}
			accounts = append(accounts, account)
		}
	}

	type Posting struct {
		Source uuid.UUID
		Amount int
	}
	postings := make([]Posting, len(accounts))

	remaining := amount
	for i, account := range accounts { // remove money from accounts
		posting_amount := Min(account.Balance, amount)
		postings[i].Amount = posting_amount
		postings[i].Source = account.Address
		remaining -= posting_amount
		sql := "update accounts set balance = balance - $1 where address = $2"
		_, err := ongoing_tx.Exec(context.Background(), sql, posting_amount, account.Address)
		if err != nil {
			return -1, err
		}
		if remaining == 0 {
			break
		}
	}

	if remaining > 0 {
		// WARN: this assumes the total balance - credits > negative_limit
		// sql := "insert into credits as c values($1, -$2) on conflict on constraint credits_pkey do update set balance = c.balance - $2"
		var credit_acc_id uuid.UUID
		sql := "insert into accounts (wallet_id, balance) values ($1, $2) returning address"
		row := ongoing_tx.QueryRow(context.Background(), sql, source, -remaining)
		err := row.Scan(&credit_acc_id)
		if err != nil {
			return -1, err
		}
		postings = append(postings, Posting{credit_acc_id, remaining})
	}

	var dest_acc_id uuid.UUID
	{
		// TODO: batch insert
		dest_update_sql := "insert into accounts (wallet_id, balance) values ($1, $2) returning address"
		ac_row := ongoing_tx.QueryRow(context.Background(), dest_update_sql, destination, amount)
		err = ac_row.Scan(&dest_acc_id)
		if err != nil {
			return -1, err
		}
	}

	tx_id := 0
	{
		insert_sql := "insert into transactions (source, destination, amount) values ($1, $2, $3) returning id"
		tx_row := ongoing_tx.QueryRow(context.Background(), insert_sql, source, destination, amount)
		err = tx_row.Scan(&tx_id)
		if err != nil {
			return -1, err
		}
	}

	for _, posting := range postings { // TODO: batch insert
		insert_sql := "insert into postings (transaction_id, source, destination, amount) values ($1, $2, $3, $4)"
		_, err = ongoing_tx.Exec(context.Background(), insert_sql, tx_id, posting.Source, dest_acc_id, posting.Amount)
		if err != nil {
			return -1, err
		}
	}

	// err = ongoing_tx.Commit(context.Background())
	// if err != nil {
	// 	return -1, err
	// }

	return tx_id, nil
}

func RevertTransaction(tx_id int) (int, error) {
	if tx_id <= 0 {
		return -1, errors.New("invalid tx_id")
	}

	type Metadata map[string]int
	var source, destination WalletId
	var metadata Metadata
	{
		sql := "select source, destination, metadata from transactions where id = $1"
		row := conn.QueryRow(context.Background(), sql, tx_id)
		err := row.Scan(&source, &destination, &metadata)
		if err != nil {
			return -1, err
		}
	}

	{ // TODO: check if tx was reverted or reverts another one
		if metadata["reverted_by"] != 0 {
			return -1, errors.New("transactions already reverted")
		}
		if metadata["reverts"] != 0 { // TODO: should we prevent this from happening?
			return -1, errors.New("transactions already reverts another one")
		}
	}

	tx, err := conn.Begin(context.Background())
	if err != nil {
		return -1, err
	}
	defer tx.Rollback(context.Background())

	err = LockWallets(tx, source, destination)
	if err != nil {
		return -1, err
	}

	var amount int
	var postings struct {
		Sources      []uuid.UUID
		Destinations []uuid.UUID
		Amounts      []int
	}
	{
		sql := "select t.amount, array_agg(p.source), array_agg(p.destination), array_agg(p.amount)" +
			" from transactions t join postings p on p.transaction_id = t.id where t.id = $1 group by t.id"
		row := tx.QueryRow(context.Background(), sql, tx_id)
		err := row.Scan(&amount, &postings.Sources, &postings.Destinations, &postings.Amounts)
		if err != nil {
			return -1, err
		}
	}

	revert_id := 0
	{
		sql := "insert into transactions (source, destination, amount, metadata) values ($1, $2, $3, $4) returning id"
		row := tx.QueryRow(context.Background(), sql, destination, source, amount, map[string]int{"reverts": tx_id})
		err := row.Scan(&revert_id)
		if err != nil {
			return -1, err
		}
	}

	{
		sql := "update transactions set metadata = jsonb_set(metadata, '{reverted_by}', $1) where id = $2"
		_, err := tx.Exec(context.Background(), sql, revert_id, tx_id)
		if err != nil {
			return -1, err
		}
	}

	for i := range postings.Amounts {
		sql := "insert into postings (transaction_id, source, destination, amount) values ($1, $2, $3, $4)"
		_, err := tx.Exec(context.Background(), sql, revert_id, postings.Destinations[i], postings.Sources[i], postings.Amounts[i])
		if err != nil {
			return -1, err
		}

		// TODO: group theese updates in less queries
		update_acc_sql := "update accounts set balance = balance + $1 where address = $2"
		_, err = tx.Exec(context.Background(), update_acc_sql, postings.Amounts[i], postings.Sources[i])
		if err != nil {
			return -1, err
		}
		update_acc_sql2 := "update accounts set balance = balance - $1 where address = $2"
		_, err = tx.Exec(context.Background(), update_acc_sql2, postings.Amounts[i], postings.Destinations[i])
		if err != nil {
			return -1, err
		}
	}

	err = tx.Commit(context.Background())
	if err != nil {
		return -1, err
	}

	return revert_id, nil
}

type Db interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func RunQuery[T any](db Db, rowToFunc pgx.RowToFunc[T], query string, args ...any) ([]T, error) {
	rows, err := db.Query(context.Background(), query, args...)
	if err != nil {
		return nil, err
	}
	t, err := pgx.CollectRows(rows, rowToFunc)
	return t, err
}

func QueryCollect(db Db, fn func(row pgx.CollectableRow) error, query string, args ...any) error {
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

func LockWallets(db Db, wallets ...WalletId) error {
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

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
