#include <php.h>
#include <SAPI.h>
#include <Zend/zend_API.h>
#include <Zend/zend_exceptions.h>
#include <string.h>

#include "cli.h"
#include "_cgo_export.h"

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_cluster_receive, 0, 1, _IS_BOOL, 0)
    ZEND_ARG_TYPE_INFO(0, handler, IS_CALLABLE, 0)
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, timeout, IS_LONG, 0, "0")
ZEND_END_ARG_INFO()

PHP_FUNCTION(cluster_receive)
{
    zend_fcall_info fci;
    zend_fcall_info_cache fcc;
    zend_long timeout = 0;
    ZEND_PARSE_PARAMETERS_START(1, 2)
        Z_PARAM_FUNC(fci, fcc)
        Z_PARAM_OPTIONAL
        Z_PARAM_LONG(timeout)
    ZEND_PARSE_PARAMETERS_END();

    struct go_cluster_receive_return next = go_cluster_receive((long) timeout);
    if (next.r2 == CLUSTER_RECEIVE_UNAVAILABLE) {
        zend_throw_error(NULL, "cluster_receive() is for CLI participants; a server gets the messages of other members in the worker of cluster member");
        RETURN_THROWS();
    }
    if (next.r2 != CLUSTER_RECEIVE_MESSAGE) {
        RETURN_FALSE;
    }

    zval event, answer;
    ZVAL_ARR(&event, (zend_array *) next.r0);
    ZVAL_UNDEF(&answer);
    fci.retval = &answer;
    fci.params = &event;
    fci.param_count = 1;

    zend_result called = zend_call_function(&fci, &fcc);
    zval_ptr_dtor(&event);

    if (called != SUCCESS || EG(exception)) {
        go_cluster_answer(next.r1, NULL, CLUSTER_ANSWER_FAILED);
        zval_ptr_dtor(&answer);
        RETURN_THROWS();
    }

    int unanswered;
    if (Z_TYPE(answer) == IS_STRING) {
        unanswered = go_cluster_answer(next.r1, Z_STR(answer), CLUSTER_ANSWER_GIVEN);
    } else if (Z_TYPE(answer) == IS_NULL) {
        unanswered = go_cluster_answer(next.r1, NULL, CLUSTER_ANSWER_GIVEN);
    } else {
        unanswered = go_cluster_answer(next.r1, NULL, CLUSTER_ANSWER_INVALID);
    }
    if (unanswered) {
        php_error_docref(NULL, E_WARNING, "The answer to a request must be a string or null, %s given", zend_zval_type_name(&answer));
    }
    zval_ptr_dtor(&answer);

    RETURN_TRUE;
}

static const zend_function_entry cluster_cli_functions[] = {
    ZEND_FE(cluster_receive, arginfo_cluster_receive)
    ZEND_FE_END
};

PHP_MINIT_FUNCTION(cluster_cli)
{
    if (strcmp(sapi_module.name, "cli") == 0) {
        go_cluster_cli_startup();
    }

    return SUCCESS;
}

PHP_MSHUTDOWN_FUNCTION(cluster_cli)
{
    go_cluster_cli_shutdown();

    return SUCCESS;
}

zend_module_entry cluster_cli_module_entry = {STANDARD_MODULE_HEADER,
                                             "cluster_cli",
                                             cluster_cli_functions,
                                             PHP_MINIT(cluster_cli),
                                             PHP_MSHUTDOWN(cluster_cli),
                                             NULL,
                                             NULL,
                                             NULL,
                                             "1.0.0",
                                             STANDARD_MODULE_PROPERTIES};
