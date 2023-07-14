create database ledger;

create table wallets
(
	address uuid primary key default gen_random_uuid(),
	metadata jsonb not null default '{}'::jsonb,
	negative_limit numeric not null default 0 check (negative_limit >= 0)
);

create table accounts
(
	address uuid primary key default gen_random_uuid(),
	metadata jsonb not null default '{}'::jsonb,
	balance numeric not null, -- how to prevent decimal?
	created timestamptz not null default now(),
	wallet_id uuid not null references wallets(address)
--	credit bool not null default false,
--	check ((credit is false and balance >= 0) or (credit is true and balance < 0))
);

create table transactions
(
	id uuid primary key default gen_random_uuid(),
	metadata jsonb not null default '{}'::jsonb,
	timestamp timestamptz not null default now()
);

create table postings
(
	id uuid primary key default gen_random_uuid(),
	transaction_id int not null references transactions(id),
	source uuid not null references accounts(address),
	destination uuid not null references accounts(address),
	amount numeric not null check (amount > 0), -- how to prevent decimal?
	check (destination != source)
);

create table revertions
(
	revert uuid not null references postings(id) unique,
	reverted uuid not null references postings(id) unique,
	primary key (revert, reverted),
	check (revert != reverted)
);


-- partial index
-- create unique index some_index on test (id) where active;

-- add trigger to block changes from happening in certain tables;
-- create function do_not_change()
--   returns trigger
-- as
-- $$
-- begin
--   raise exception 'Cannot modify table. 
-- Contact the system administrator if you want to make this change.';
-- end;
-- $$
-- language plpgsql;
-- create trigger no_change_trigger
-- before update or delete on "transactions"
-- execute procedure do_not_change();