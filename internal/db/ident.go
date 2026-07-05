package db

import "strings"

// QuoteIdent returns name as a backtick-quoted MySQL identifier, doubling
// any embedded backticks. All schema/table/column names woven into DDL
// must pass through here (the stored-procedure edition concatenated them
// raw — review finding F8).
func QuoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// QuoteQualified returns `schema`.`object`.
func QuoteQualified(schema, object string) string {
	return QuoteIdent(schema) + "." + QuoteIdent(object)
}
