#ifndef _CLUSTER_CLI_H
#define _CLUSTER_CLI_H

#include <php.h>

extern zend_module_entry cluster_cli_module_entry;

#define CLUSTER_RECEIVE_MESSAGE 0
#define CLUSTER_RECEIVE_NONE 1
#define CLUSTER_RECEIVE_UNAVAILABLE 2

#define CLUSTER_ANSWER_FAILED 0
#define CLUSTER_ANSWER_GIVEN 1
#define CLUSTER_ANSWER_INVALID 2

#endif
