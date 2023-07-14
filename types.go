package main

import (
	"database/sql/driver"
	"errors"
	"math/big"

	"github.com/google/uuid"
)

var ZERO = NewBigInt(0)

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

//	type BigInt struct {
//		Int *big.Int
//	}
type BigInt big.Int

func NewBigInt(i int64) *BigInt {
	return (*BigInt)(big.NewInt(i))
}

func CopyBigInt(x *BigInt) *BigInt {
	b := NewBigInt(0)
	(*big.Int)(b).Set((*big.Int)(x))
	return b
}

func (b *BigInt) String() string {
	return (*big.Int)(b).String()
}
func (b *BigInt) Scan(src any) error {
	if b == nil {
		b = NewBigInt(0)
	}
	i, ok := (*big.Int)(b).SetString(src.(string), 10)
	if !ok {
		return errors.New("unable to convert string to big.Int")
	}
	b = (*BigInt)(i)
	return nil
}

func (b *BigInt) Add(a *BigInt) {
	(*big.Int)(b).Add((*big.Int)(b), (*big.Int)(a))
	return
}
func (b *BigInt) Sub(a *BigInt) {
	(*big.Int)(b).Sub((*big.Int)(b), (*big.Int)(a))
	return
}
func (b *BigInt) Cmp(a *BigInt) int {
	return (*big.Int)(b).Cmp((*big.Int)(a))
}
func (b *BigInt) Neg() {
	(*big.Int)(b).Neg((*big.Int)(b))
	return
}
func (b *BigInt) Set(x *BigInt) {
	(*big.Int)(b).Set((*big.Int)(x))
	return
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
