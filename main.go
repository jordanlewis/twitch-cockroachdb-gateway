package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/gempir/go-twitch-irc/v2"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

var stmts = []string{
	"ADD",
	"ALTER",
	//"BACKUP",
	"CANCEL",
	"COMMENT",
	//"COPY",
	"CREATE",
	//"DEALLOCATE",
	"DELETE",
	"DISCARD",
	"DROP",
	"EXECUTE",
	"EXPLAIN",
	//"EXPORT",
	//"IMPORT",
	"INSERT",
	//"GRANT",
	"PAUSE",
	"PREPARE",
	"RELEASE",
	"RESET",
	//"REVOKE",
	"SAVEPOINT",
	"SELECT",
	//"SET",
	"SHOW",
	"TRUNCATE",
	"UPDATE",
	"UPSERT",
}

var stmtsMap map[string]struct{}

func init() {
	stmtsMap = make(map[string]struct{})
	for _, s := range stmts {
		stmtsMap[s] = struct{}{}
	}
}

func main() {
	ctx := context.Background()
	client := twitch.NewAnonymousClient()
	client.Join("twitchrunscockroachdb")
	url := os.Getenv("DATABASE_URL")
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		panic(err)
	}
	config.BeforeAcquire = func(ctx context.Context, conn *pgx.Conn) bool {
		_, err := conn.Exec(ctx, "SET statement_timeout='5s'")
		if err != nil {
			fmt.Println("error acquiring connection", err)
			return false
		}
		return true
	}
	pool, err := pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		panic(err)
	}

	/*
		conn, err := pool.Acquire(ctx)
		if err != nil {
			panic(err)
		}
		//connInfo := conn.Conn().ConnInfo()
		conn.Release()
	*/

	processMessage := func(message twitch.PrivateMessage) {
		if err := func() error {
			tokens := strings.Split(message.Message, " ")
			if _, ok := stmtsMap[strings.ToUpper(tokens[0])]; ok {
				fmt.Printf("%s: `%s`\n", message.User.DisplayName, message.Message)
				ctx, _ := context.WithTimeout(ctx, 10*time.Second)
				rows, err := pool.Query(ctx, message.Message, pgx.QuerySimpleProtocol(true))
				if err != nil {
					return err
				}
				i := 0
				var descriptions []pgproto3.FieldDescription
				for rows.Next() {
					if i == 0 {
						sb := strings.Builder{}
						descriptions = rows.FieldDescriptions()
						for i, col := range descriptions {
							if i > 0 {
								sb.WriteRune(',')
							}
							sb.WriteString(string(col.Name))
						}
						fmt.Println(sb.String())
					}
					i++
					if i > 10 {
						rows.Close()
						break
					}

					/*
						if err != nil {
							return err
						}
					*/
					row := rows.RawValues()
					buf := make([]byte, 0, len(row)*10)
					for i, v := range row {
						if i > 0 {
							buf = append(buf, ',')
						}
						buf = append(buf, v...)
						//buf, err = formatVal(buf, connInfo, v)
						//if err != nil {
						//	return err
						//}
					}
					fmt.Println(string(buf))
				}
				if err := rows.Err(); err != nil {
					return err
				}
				if i > 10 {
					fmt.Printf("... more rows ...\n")
				}
				if i == 0 && !rows.CommandTag().Select() {
					// Print the command tag if it wasn't a select.
					fmt.Println(rows.CommandTag())
				}

			}
			return nil
		}(); err != nil {
			fmt.Println(err)
		}
	}
	client.OnPrivateMessage(processMessage)

	/*
		processMessage(twitch.PrivateMessage{
			User:    twitch.User{DisplayName: "jordan"},
			Message: "select 'foo'::bytea, gen_random_uuid(), now()::time",
		})
	*/

	if err := client.Connect(); err != nil {
		panic(err)
	}
}

func encodeByteArrayToRawBytes(data string) string {
	// PostgreSQL does not allow all the escapes formats recognized by
	// CockroachDB's scanner. It only recognizes octal and \\ for the
	// backslash itself.
	// See https://www.postgresql.org/docs/current/static/datatype-binary.html#AEN5667
	res := make([]byte, 0, len(data))
	for _, c := range []byte(data) {
		if c == '\\' {
			res = append(res, '\\', '\\')
		} else if c < 32 || c >= 127 {
			// Escape the character in octal.
			//
			// Note: CockroachDB only supports UTF-8 for which all values
			// below 128 are ASCII. There is no locale-dependent escaping
			// in that case.
			res = append(res, '\\', '0'+(c>>6), '0'+((c>>3)&7), '0'+(c&7))
		} else {
			res = append(res, c)
		}
	}
	return string(res)

}

func formatVal(buf []byte, connInfo *pgtype.ConnInfo, v interface{}) ([]byte, error) {
	if encoder, ok := v.(pgtype.TextEncoder); ok {
		return encoder.EncodeText(connInfo, buf)
	}
	buf = append(buf, []byte(fmt.Sprintf("%v", v))...)
	return buf, nil
}

const timestampOutputFormat = "2006-01-02 15:04:05.999999-07:00"
