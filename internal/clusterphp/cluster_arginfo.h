/* This is a generated file, edit the .stub.php file instead.
 * Stub hash: 32206b4bc83be08eb4c313d591d4c6d53aab9c01 */

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_cluster_self, 0, 0, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_cluster_members, 0, 0, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

#define arginfo_cluster_member_info arginfo_cluster_members

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_cluster_send, 0, 2, _IS_BOOL, 0)
	ZEND_ARG_TYPE_INFO(0, instance, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, data, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_cluster_publish, 0, 1, IS_LONG, 0)
	ZEND_ARG_TYPE_INFO(0, data, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_cluster_request, 0, 2, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, instance, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, data, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, timeout, IS_LONG, 0, "0")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_cluster_last_error, 0, 0, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_FUNCTION(cluster_self);
ZEND_FUNCTION(cluster_members);
ZEND_FUNCTION(cluster_member_info);
ZEND_FUNCTION(cluster_send);
ZEND_FUNCTION(cluster_publish);
ZEND_FUNCTION(cluster_request);
ZEND_FUNCTION(cluster_last_error);

static const zend_function_entry ext_functions[] = {
	ZEND_FE(cluster_self, arginfo_cluster_self)
	ZEND_FE(cluster_members, arginfo_cluster_members)
	ZEND_FE(cluster_member_info, arginfo_cluster_member_info)
	ZEND_FE(cluster_send, arginfo_cluster_send)
	ZEND_FE(cluster_publish, arginfo_cluster_publish)
	ZEND_FE(cluster_request, arginfo_cluster_request)
	ZEND_FE(cluster_last_error, arginfo_cluster_last_error)
	ZEND_FE_END
};

static void register_cluster_symbols(int module_number)
{
	REGISTER_LONG_CONSTANT("CLUSTER_ERROR_NONE", 0, CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("CLUSTER_ERROR_NOT_JOINED", 1, CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("CLUSTER_ERROR_INVALID_ARGUMENT", 2, CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("CLUSTER_ERROR_BUS_FAILED", 3, CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("CLUSTER_ERROR_TIMEOUT", 4, CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("CLUSTER_ERROR_NO_RECEIVER", 5, CONST_PERSISTENT);
}
