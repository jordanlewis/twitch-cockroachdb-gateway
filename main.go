package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/gempir/go-twitch-irc/v2"
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
	pool, err := pgxpool.Connect(ctx, url)
	if err != nil {
		panic(err)
	}

	//connInfo := pgtype.NewConnInfo()
	processMessage := func(message twitch.PrivateMessage) {
		tokens := strings.Split(message.Message, " ")
		if _, ok := stmtsMap[strings.ToUpper(tokens[0])]; ok {
			fmt.Printf("%s: `%s`\n", message.User.DisplayName, message.Message)
			ctx, _ := context.WithTimeout(ctx, 10*time.Second)
			rows, err := pool.Query(ctx, message.Message)
			if err != nil {
				fmt.Println(err)
				return
			}
			i := 0
			for rows.Next() {
				if i == 0 {
					sb := strings.Builder{}
					for i, col := range rows.FieldDescriptions() {
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

				sb := strings.Builder{}
				row, err := rows.Values()
				if err != nil {
					fmt.Println(err)
					return
				}
				for i, v := range row {
					if i > 0 {
						sb.WriteRune(',')
					}
					sb.WriteString(fmt.Sprintf("%v", v))
				}
				fmt.Println(sb.String())
			}
			if err := rows.Err(); err != nil {
				fmt.Println(err)
				return
			}
			if i > 10 {
				fmt.Printf("... more rows ...\n")
			}
			if i == 0 && !rows.CommandTag().Select() {
				// Print the command tag if it wasn't a select.
				fmt.Println(rows.CommandTag())
			}

		}
	}
	client.OnPrivateMessage(processMessage)

	if err := client.Connect(); err != nil {
		panic(err)
	}
}
