#include "utils.h"

#include <ctype.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define ANSI_COLOR_RED "\x1b[31m"
#define ANSI_COLOR_GREEN "\x1b[32m"
#define ANSI_COLOR_RESET "\x1b[0m"

#define LOG 0
#define LOG_ERR 0
#define LOG_INFO 0

char *strsep(char **stringp, const char *delim) {
  char *rv = *stringp;
  if (rv) {
    *stringp += strcspn(*stringp, delim);
    if (**stringp)
      *(*stringp)++ = '\0';
    else
      *stringp = 0;
  }
  return rv;
}

/*
 * Destructively splits a string in format a.b and returns a and modifies
 * original to b
 */
char *split_table_column(char **tokenizer) {
  // printf("splitting string on dot %s\n", *tokenizer );
  char *token = strsep(tokenizer, ".");
  return token;
}

/* removes newline characters from the input string.
 * Shifts characters over and shortens the length of
 * the string by the number of newline characters.
 */
char *trim_newline(char *str) {
  int length = strlen(str);
  int current = 0;
  for (int i = 0; i < length; ++i) {
    if (!(str[i] == '\r' || str[i] == '\n')) {
      str[current++] = str[i];
    }
  }

  // Write new null terminator
  str[current] = '\0';
  return str;
}
/* removes space characters from the input string.
 * Shifts characters over and shortens the length of
 * the string by the number of space characters.
 */
char *trim_whitespace(char *str) {
  int length = strlen(str);
  int current = 0;
  for (int i = 0; i < length; ++i) {
    if (!isspace(str[i])) {
      str[current++] = str[i];
    }
  }

  // Write new null terminator
  str[current] = '\0';
  return str;
}

/* removes parenthesis characters from the input string.
 * Shifts characters over and shortens the length of
 * the string by the number of parenthesis characters.
 */
char *trim_parenthesis(char *str) {
  int length = strlen(str);
  int current = 0;
  for (int i = 0; i < length; ++i) {
    if (!(str[i] == '(' || str[i] == ')')) {
      str[current++] = str[i];
    }
  }

  // Write new null terminator
  str[current] = '\0';
  return str;
}

char *trim_quotes(char *str) {
  int length = strlen(str);
  int current = 0;
  for (int i = 0; i < length; ++i) {
    if (str[i] != '\"') {
      str[current++] = str[i];
    }
  }

  // Write new null terminator
  str[current] = '\0';
  return str;
}
/* The following two functions will show output on the terminal
 * based off whether the corresponding level is defined.
 * To see error output, define LOG_ERR.
 * To see info output, define LOG_INFO
 */

void log_err(const char *format, ...) {
#ifdef LOG_ERR
  va_list v;
  va_start(v, format);
  fprintf(stderr, ANSI_COLOR_RED);
  vfprintf(stderr, format, v);
  fprintf(stderr, ANSI_COLOR_RESET);
  va_end(v);
#else
  (void)format;
#endif
}

void log_info(const char *format, ...) {
#ifdef LOG_INFO
  va_list v;
  va_start(v, format);
  fprintf(stdout, ANSI_COLOR_GREEN);
  vfprintf(stdout, format, v);
  fprintf(stdout, ANSI_COLOR_RESET);
  fflush(stdout);
  va_end(v);
#else
  (void)format;
#endif
}
