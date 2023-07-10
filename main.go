package main

import (
	"context"
	"errors"
	"log"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var conn *pgx.Conn

func main() {
	var err error
	conn, err = pgx.Connect(context.Background(), "postgresql://ledger_gateway:ledger_gateway@localhost:5432/ledger")
	if err != nil {
		log.Fatalln(err)
	}
	defer conn.Close(context.Background())

	{ // TEST CODE
		source := uuid.New()
		dest := uuid.New()
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

		tx_id, err := Transact(source, dest, amount)
		if err != nil {
			log.Fatalln(err)
		}
		revert_id, err := RevertTransaction(tx_id)
		if err != nil {
			log.Fatalln(err)
		}

		_, err = RevertTransaction(tx_id)
		if err != nil {
			log.Println(err)
		}
		_, err = RevertTransaction(revert_id)
		if err != nil {
			log.Println(err)
		}
	}

}

func Transact(source, destination uuid.UUID, amount int /* TODO: what data type should we use for precision? WARN*/) (int, error) {
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

	tx, err := conn.Begin(context.Background())
	if err != nil {
		return -1, err
	}
	defer tx.Rollback(context.Background()) // TODO: ignore the error?

	// TODO: lock accounts? How can I prevent an account to be created with wallet_id in (source, destination) during this transaction?
	err = LockWallets(tx, source, destination)
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
		rows, err := tx.Query(context.Background(), select_accounts_sql, source.String())
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
		_, err := tx.Exec(context.Background(), sql, posting_amount, account.Address)
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
		row := tx.QueryRow(context.Background(), sql, source, -remaining)
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
		ac_row := tx.QueryRow(context.Background(), dest_update_sql, destination, amount)
		err = ac_row.Scan(&dest_acc_id)
		if err != nil {
			return -1, err
		}
	}

	tx_id := 0
	{
		insert_sql := "insert into transactions (source, destination, amount) values ($1, $2, $3) returning id"
		tx_row := tx.QueryRow(context.Background(), insert_sql, source, destination, amount)
		err = tx_row.Scan(&tx_id)
		if err != nil {
			return -1, err
		}
	}

	for _, posting := range postings { // TODO: batch insert
		insert_sql := "insert into postings (transaction_id, source, destination, amount) values ($1, $2, $3, $4)"
		_, err = tx.Exec(context.Background(), insert_sql, tx_id, posting.Source, dest_acc_id, posting.Amount)
		if err != nil {
			return -1, err
		}
	}

	err = tx.Commit(context.Background())
	if err != nil {
		return -1, err
	}

	return tx_id, nil
}

func RevertTransaction(tx_id int) (int, error) {
	if tx_id <= 0 {
		return -1, errors.New("invalid tx_id")
	}

	type Metadata map[string]int
	var source uuid.UUID
	var destination uuid.UUID
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

func LockWallets(tx pgx.Tx, source, destination uuid.UUID) error {
	// WARN: this doesnt prevent accounts from theese wallets to be altered or created. This is here to prevent this function to run concurrently on the same wallets
	// NOTE: copied the idea from https://stackoverflow.com/a/52557413/13449544
	// NOTE: this doensn't lock from read... TODO: should we lock from read?
	_, err := tx.Exec(context.Background(), "lock table wallets in row exclusive mode")
	if err != nil {
		return err
	}
	_, err = tx.Exec(context.Background(), "select * from wallets where address in ($1, $2) for update", source, destination)
	return err
}

func CheckWalletExists(wallet_id uuid.UUID) bool {
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
