// module represents custom parsers

package api

import (
	"errors"
	"strings"
)

func parseTransactions(request string) ([]string, error) {
	var transactions []string
	b := strings.Builder{}
	skipStringLen := len(`{"transactions":[`)
	if len(request) < skipStringLen {
		return nil, errors.New("length of request string is too small")
	}
	transactionsString := request[skipStringLen : len(request)-1]
	bracketsCount := 0
	for _, ch := range transactionsString {
		if ch == '{' {
			bracketsCount++
		}
		if ch == '}' {
			bracketsCount--
			// json object was closed
			if bracketsCount == 0 {
				// dont forget append the last bracket
				b.WriteRune(ch)
				transactions = append(transactions, b.String())
				// reset for retrieving the next object
				b.Reset()
			}
		}
		// do not add runes between brackets
		if bracketsCount > 0 {
			b.WriteRune(ch)
		}
	}

	return transactions, nil
}
