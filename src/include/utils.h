// utils.h
// CS165 Fall 2015
//
// Provides utility and helper functions that may be useful throughout.
// Includes debugging tools.

#ifndef __UTILS_H__
#define __UTILS_H__

#include <stdarg.h>
#include <stdio.h>

/**
* Splits string a.b, return a and modfying tokenizer to b
**/

char* split_table_column(char** tokenizer) ;

/**
 * trims newline characters from a string (in place)
 **/

char* trim_newline(char *str);

/**
 * trims parenthesis characters from a string (in place)
 **/

char* trim_parenthesis(char *str);

/**
 * trims whitespace characters from a string (in place)
 **/

char* trim_whitespace(char *str);

/**
 * trims quotations characters from a string (in place)
 **/

char* trim_quotes(char *str);

/**
 * @brief implementation of strsep without using gnu99 c for portability. Original man description:
 * If *stringp is NULL, the strsep() function returns NULL and does
 *  nothing else.  Otherwise, this function finds the first token in the
 *  string *stringp, that is delimited by one of the bytes in the string
 *  delim.  This token is terminated by overwriting the delimiter with a
 *  null byte ('\0'), and *stringp is updated to point past the token.
 *  In case no delimiter was found, the token is taken to be the entire
 *  string *stringp, and *stringp is made NULL.
 * @return a pointer to the token, that is, it
       returns the original value of *stringp.
 */
char *strsep(char **stringp, const char *delim);


// log_err(format, ...)
// Writes the string from @format to stderr, extendable for
// additional parameters.
//
// Usage: log_err("%s: error at line: %d", __func__, __LINE__);
void log_err(const char *format, ...);

// log_info(format, ...)
// Writes the string from @format to stdout, extendable for
// additional parameters.
//
// Usage: log_info("Command received: %s", command_string);
void log_info(const char *format, ...);

#endif /* __UTILS_H__ */
