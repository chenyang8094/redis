#include "redismodule.h"
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <stdint.h>

static RedisModuleType *DbCntType;

#define MAX_DB 16
size_t dbcnt[MAX_DB];

typedef struct DbCntObject {
    RedisModuleString *value;
} DbCntObject;

DbCntObject *createDbCntObject(void) {
    DbCntObject *o = RedisModule_Alloc(sizeof(*o));
    o->value = NULL;
    return o;
}

void DbCntReleaseObject(DbCntObject *o) {
    if (o->value) {
        RedisModule_FreeString(NULL, o->value);
    }
    RedisModule_Free(o);
}

void swapDbCallback(RedisModuleCtx *ctx, RedisModuleEvent e, uint64_t sub, void *data) {
    REDISMODULE_NOT_USED(ctx);
    REDISMODULE_NOT_USED(e);
    REDISMODULE_NOT_USED(sub);

    RedisModuleSwapDbInfo *ei = data;

    size_t tmp = dbcnt[ei->dbnum_first];
    dbcnt[ei->dbnum_first] = dbcnt[ei->dbnum_second];
    dbcnt[ei->dbnum_second] = tmp;
}

RedisModuleString *takeAndRef(RedisModuleString *str) {
    RedisModule_RetainString(NULL, str);
    return str;
}

/* DBCNT.SET key value */
int DbCntSet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);  

    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ|REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != DbCntType) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    DbCntObject *o;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        o = createDbCntObject();
        RedisModule_ModuleTypeSetValue(key, DbCntType, o);
    } else {
        o = RedisModule_ModuleTypeGetValue(key);
    }

    if (o->value) {
        RedisModule_FreeString(NULL, o->value);
    }
    o->value = takeAndRef(argv[2]);

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

/* DBCNT.GET key */
int DbCntGet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

    if (argc != 2) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ|REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != DbCntType) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    DbCntObject *o;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithNull(ctx);
    } else {
        o = RedisModule_ModuleTypeGetValue(key);
    }

    RedisModule_ReplyWithString(ctx, o->value);
    return REDISMODULE_OK;
}

/* DBCNT.SIZE dbid */
int DbCntSize_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

    if (argc != 2) {
        return RedisModule_WrongArity(ctx);
    }

    int dbid;
    if ((RedisModule_StringToLongLong(argv[1], (long long *)&dbid) != REDISMODULE_OK)) {
        return RedisModule_ReplyWithError(ctx, "ERR invalid value: must be a signed 64 bit integer");
    }

    RedisModule_ReplyWithLongLong(ctx, dbcnt[dbid]);
    return REDISMODULE_OK;
}

void *DbCntRdbLoad(RedisModuleIO *rdb, int encver) {
    if (encver != 0) {
        return NULL;
    }

    DbCntObject *o = createDbCntObject();
    o->value = RedisModule_LoadString(rdb);
    
    /*  */
    int dbid = RedisModule_GetDbFromIO(rdb);
    dbcnt[dbid]++;
    return o;
}

void DbCntRdbSave(RedisModuleIO *rdb, void *value) {
    DbCntObject *o = value;
    RedisModule_SaveString(rdb, o->value);
}

void DbCntAofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    DbCntObject *o = (DbCntObject *)value;
    RedisModule_EmitAOF(aof, "DBCNT.SET", "ss", key, o->value);
}

void DbCntFree(void *value) {
    DbCntReleaseObject(value);
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_Init(ctx, "lazyfreetest", 1,REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    /* We only allow our module to be loaded when the redis core version is greater than the version of my module */
    if (RedisModule_GetTypeMethodVersion() < REDISMODULE_TYPE_METHOD_VERSION) {
        return REDISMODULE_ERR;
    }

    RedisModuleTypeMethods tm = {
        .version = REDISMODULE_TYPE_METHOD_VERSION,
        .rdb_load = DbCntRdbLoad,
        .rdb_save = DbCntRdbSave,
        .aof_rewrite = DbCntAofRewrite,
        .free = DbCntFree,
    };

    DbCntType = RedisModule_CreateDataType(ctx, "test_getdb", 0, &tm);
    if (DbCntType == NULL) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "dbcnt.set", DbCntSet_RedisCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "dbcnt.get", DbCntGet_RedisCommand, "readonly", 1, 1, 1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "dbcnt.size", DbCntSize_RedisCommand, "readonly", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_SwapDB, swapDbCallback);
    return REDISMODULE_OK;
}
