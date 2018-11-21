package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
)

func main() {
	var (
		project  = os.Getenv("SPANNER_PROJECT")
		instance = os.Getenv("SPANNER_INSTANCE")
		db       = os.Getenv("SPANNER_DATABASE")
	)
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, db))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	params := []Param{
		{
			Query: `SELECT * FROM MyTable@{FORCE_INDEX=MyTableBySI} WHERE SI = @param`,
			Value: "si",
		},
		{
			Query: `SELECT * FROM MyTable@{FORCE_INDEX=MyTableBySI} WHERE SI = @param`,
			Value: uuid.New().String(),
		},
		{
			Query: `SELECT * FROM MyTable@{FORCE_INDEX=MyTableBySI} WHERE SI > @param`,
			Value: "aa",
		},
		{
			Query: `SELECT * FROM MyTable@{FORCE_INDEX=MyTableBySI} WHERE SI > @param`,
			Value: "zz",
		},
		{
			Query: `SELECT * FROM MyTable WHERE PK = @param`,
			Value: uuid.New().String(),
		},
		{
			Query: `SELECT * FROM MyTable WHERE PK = @param`,
			Value: "pk",
		},
		{
			Query: `SELECT * FROM MyTable WHERE PK > @param`,
			Value: "aa",
		},
		{
			Query: `SELECT * FROM MyTable WHERE PK > @param`,
			Value: "pk",
		},
		{
			Query: `SELECT * FROM MyTable WHERE PK > @param`,
			Value: "zz",
		},
	}
	for _, p := range params {
		ctx := context.Background()
		if err := process(ctx, client, p); err != nil {
			log.Printf("failed %+v. err = %v\n", p, err)
		} else {
			log.Printf("success %+v\n", p)
		}
	}
}

type Param struct {
	Query string
	Value string
}

func process(ctx context.Context, client *spanner.Client, p Param) error {
	m := spanner.Insert("MyTable",
		[]string{"PK", "SI", "V"},
		[]interface{}{"pk", "si", "v"},
	)
	st := spanner.NewStatement(p.Query)
	st.Params["param"] = p.Value

	eg := errgroup.Group{}
	for i := 0; i < 100; i++ {
		i := i
		eg.Go(func() error {
			if _, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
				if err := txn.Query(ctx, st).Do(func(_ *spanner.Row) error { return nil }); err != nil {
					return errors.Wrap(err, fmt.Sprintf("%v : txn.Query", i)) // wrap error in order to avoid automatically retry.
				}
				return txn.BufferWrite([]*spanner.Mutation{m})
			}); err != nil && spanner.ErrCode(err) != codes.AlreadyExists {
				return errors.Wrap(err, fmt.Sprintf("%v : txn.Commit", i)) // wrap error in order to avoid automatically retry.
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}
