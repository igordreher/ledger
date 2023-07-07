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
		_, err := conn.Exec(context.Background(), "insert into wallets (address) values ($1), ($2)", source, dest)
		if err != nil {
			log.Fatal(err)
		}

		_, err = conn.Exec(context.Background(), "insert into accounts (balance, wallet_id) values($1, $2)", amount, source)
		if err != nil {
			log.Fatal(err)
		}

		err = Transact(source, dest, amount)
		if err != nil {
			log.Fatalln(err)
		}
	}
}

func Transact(source, destination uuid.UUID, amount int /* TODO: what data type should we use for precision? WARN*/) error {
	if amount <= 0 {
		return errors.New("invalid amount")
	}

	if !CheckWalletExists(source) {
		return errors.New("source wallet doesn't exist")
	}
	if !CheckWalletExists(destination) {
		return errors.New("destination wallet doesn't exist")
	}

	var balance int
	{
		// TODO: handle negative limit
		sum_balance_sql := "select sum(balance) from wallets where address = $1 group by w.address"
		// sum_balance_sql := "select sum(balance) + w.negative_limit from wallets w " +
		// 	"join accounts a on a.wallet_id = w.address " +
		// 	"where w.address = $1 group by w.address"
		row := conn.QueryRow(context.Background(), sum_balance_sql, source.String())
		err := row.Scan(&balance)
		if err != nil {
			return err
		}
	}

	if balance < amount {
		return errors.New("unsificient funds")
	}

	tx, err := conn.Begin(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background()) // TODO: ignore the error?

	{ // lock all accounts from the postings NOTE: copied the idea from https://stackoverflow.com/a/52557413/13449544
		// WARN: this doesnt prevent accounts from theese wallets to be altered or created. This is here to prevent this function to run concurrently on the same wallets
		// TODO: lock accounts? How can I prevent an account to be created with wallet_id in (source, destination) during this transaction?
		// NOTE: this doensn't lock from read... TODO: should we lock from read?
		_, err := tx.Exec(context.Background(), "lock table wallets in row exclusive mode")
		if err != nil {
			return err
		}
		_, err = tx.Exec(context.Background(), "select * from wallets where address in ($1, $2) for update", source, destination)
		if err != nil {
			return err
		}
	}

	var balances []int
	var sources []uuid.UUID
	{
		select_accounts_sql := "select address, balance from accounts where wallet_id = $1 order by created asc"
		rows, err := tx.Query(context.Background(), select_accounts_sql, source.String())
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var balance int
			var address uuid.UUID
			err = rows.Scan(&address, &balance)
			if err != nil {
				return err
			}
			balances = append(balances, balance)
			sources = append(sources, address)
		}
	}

	posting_amounts := make([]int, len(sources))

	remaining := amount
	for i := range sources { // remove money from accounts
		// TODO: handle negative limit
		posting_amount := Min(balances[i], amount)
		posting_amounts[i] = posting_amount
		remaining -= posting_amount
		sql := "update accounts set balance = $1 where address = $2"
		_, err := tx.Exec(context.Background(), sql, balances[i]-posting_amount, sources[i])
		if err != nil {
			return err
		}
		if remaining == 0 {
			break
		}
	}

	var dest_acc_id uuid.UUID
	{
		dest_update_sql := "insert into accounts (balance, wallet_id) values ($1, $2) returning address"
		ac_row := tx.QueryRow(context.Background(), dest_update_sql, amount, destination)
		err = ac_row.Scan(&dest_acc_id)
		if err != nil {
			return err
		}
	}

	tx_id := 0
	{
		insert_sql := "insert into transactions (source, destination, amount) values ($1, $2, $3) returning id"
		tx_row := tx.QueryRow(context.Background(), insert_sql, source, destination, amount)
		err = tx_row.Scan(&tx_id)
		if err != nil {
			return err
		}
	}

	for i, source := range sources { // TODO: batch insert
		insert_sql := "insert into postings (transaction_id, source, destination, amount) values ($1, $2, $3, $4)"
		_, err = tx.Exec(context.Background(), insert_sql, tx_id, source, dest_acc_id, posting_amounts[i])
		if err != nil {
			return err
		}
	}

	err = tx.Commit(context.Background())
	if err != nil {
		return err
	}

	return nil
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
