/* This is a generated file, edit the .stub.php file instead.
 * Stub hash: 13d0e36e1c4c5ad270b28c326eedfb171f1e0a49 */

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_websocket_send, 0, 2, _IS_BOOL, 0)
	ZEND_ARG_TYPE_INFO(0, connection, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, data, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, type, IS_LONG, 0, "0")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_websocket_send_to_tag, 0, 2, IS_LONG, 0)
	ZEND_ARG_TYPE_INFO(0, tag, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, data, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, type, IS_LONG, 0, "0")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_websocket_broadcast, 0, 1, IS_LONG, 0)
	ZEND_ARG_TYPE_INFO(0, data, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, type, IS_LONG, 0, "0")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_websocket_ping, 0, 1, _IS_BOOL, 0)
	ZEND_ARG_TYPE_INFO(0, connection, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, payload, IS_STRING, 0, "\"\"")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_websocket_close, 0, 1, _IS_BOOL, 0)
	ZEND_ARG_TYPE_INFO(0, connection, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, code, IS_LONG, 0, "1000")
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, reason, IS_STRING, 0, "\"\"")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_websocket_is_open, 0, 1, _IS_BOOL, 0)
	ZEND_ARG_TYPE_INFO(0, connection, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_websocket_connections, 0, 0, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_websocket_set_attribute, 0, 3, _IS_BOOL, 0)
	ZEND_ARG_TYPE_INFO(0, connection, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, name, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, value, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_websocket_remove_attribute, 0, 2, _IS_BOOL, 0)
	ZEND_ARG_TYPE_INFO(0, connection, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, name, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_websocket_attributes, 0, 1, IS_ARRAY, 0)
	ZEND_ARG_TYPE_INFO(0, connection, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_websocket_tag, 0, 2, _IS_BOOL, 0)
	ZEND_ARG_TYPE_INFO(0, connection, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, tag, IS_STRING, 0)
ZEND_END_ARG_INFO()

#define arginfo_websocket_untag arginfo_websocket_tag

#define arginfo_websocket_tags arginfo_websocket_attributes

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_websocket_last_error, 0, 0, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_FUNCTION(websocket_send);
ZEND_FUNCTION(websocket_send_to_tag);
ZEND_FUNCTION(websocket_broadcast);
ZEND_FUNCTION(websocket_ping);
ZEND_FUNCTION(websocket_close);
ZEND_FUNCTION(websocket_is_open);
ZEND_FUNCTION(websocket_connections);
ZEND_FUNCTION(websocket_set_attribute);
ZEND_FUNCTION(websocket_remove_attribute);
ZEND_FUNCTION(websocket_attributes);
ZEND_FUNCTION(websocket_tag);
ZEND_FUNCTION(websocket_untag);
ZEND_FUNCTION(websocket_tags);
ZEND_FUNCTION(websocket_last_error);

static const zend_function_entry ext_functions[] = {
	ZEND_FE(websocket_send, arginfo_websocket_send)
	ZEND_FE(websocket_send_to_tag, arginfo_websocket_send_to_tag)
	ZEND_FE(websocket_broadcast, arginfo_websocket_broadcast)
	ZEND_FE(websocket_ping, arginfo_websocket_ping)
	ZEND_FE(websocket_close, arginfo_websocket_close)
	ZEND_FE(websocket_is_open, arginfo_websocket_is_open)
	ZEND_FE(websocket_connections, arginfo_websocket_connections)
	ZEND_FE(websocket_set_attribute, arginfo_websocket_set_attribute)
	ZEND_FE(websocket_remove_attribute, arginfo_websocket_remove_attribute)
	ZEND_FE(websocket_attributes, arginfo_websocket_attributes)
	ZEND_FE(websocket_tag, arginfo_websocket_tag)
	ZEND_FE(websocket_untag, arginfo_websocket_untag)
	ZEND_FE(websocket_tags, arginfo_websocket_tags)
	ZEND_FE(websocket_last_error, arginfo_websocket_last_error)
	ZEND_FE_END
};

static void register_websocket_symbols(int module_number)
{
	REGISTER_LONG_CONSTANT("WEBSOCKET_AUTO", 0, CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("WEBSOCKET_TEXT", 1, CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("WEBSOCKET_BINARY", 2, CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("WEBSOCKET_ERROR_NONE", 0, CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("WEBSOCKET_ERROR_NOT_FOUND", 1, CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("WEBSOCKET_ERROR_NOT_OPEN", 2, CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("WEBSOCKET_ERROR_INVALID_ARGUMENT", 3, CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("WEBSOCKET_ERROR_WRITE_FAILED", 4, CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("WEBSOCKET_ERROR_BUS_FAILED", 5, CONST_PERSISTENT);
	REGISTER_LONG_CONSTANT("WEBSOCKET_ERROR_TIMEOUT", 6, CONST_PERSISTENT);
}
