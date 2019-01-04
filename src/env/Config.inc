<?php
    // [戈畐硈絬砞﹚] ==========================================================================================================
    const MYSQL_ADDRESS = "localhost";      // Host
    const MYSQL_USER_NAME = "root";         // 眀腹
    const MYSQL_PASSWORD = "root1234";      // 盞絏
    const MYSQL_DB_NAME = "apt_parsing";    // 戈畐嘿
    // ======================================================================================================================

    // [Parsing 郎砞﹚] ======================================================================================================
    const PARSING_FILE_lOCAL_PATH = "D:/Work/Hwacom_地筿/@CASE/2018.09_ㄈび-ePDG_Parsing/Files/source/";    // 郎隔畖
    const PARSING_FILE_IGNORE_ROW_COUNT = 1;            // 砞﹚郎ず甧秨繷┛菠ぃ矪瞶︽计
    const PARSING_HOST_NAME_SPLIT_SYMBOLS = array(      // 砞﹚ノㄓ秆猂郎眔[HOST_NAME]㏕﹚泊 (ノ泊ち澄郎皚[0]=HOST_NAME)
        "_bulkstats"
    );
    const END_OF_FILE = "EndOfFile";                    // 郎挡Ю泊
    const ABNORMAL_SYMBOLS = array (                    // 砞﹚讽逆篈ゅτㄓ方郎ず甧狦皚いㄤ泊玥糶DBず甧锣传[ABNORMAL_SYMBOL_TRANS_TO]
        "N/A"                                   
    );
    const ABNORMAL_SYMBOL_TRANS_TO = "";                // 钡[ABNORMAL_SYMBOLS]砞﹚讽獶タ盽ず甧璶锣传ぐ或泊
    // =======================================================================================================================
    
    // [CDR郎眔FTP硈絬砞﹚] ===================================================================================================
    const CDR_FTP_HOST_ADDRESS = "A";
    const CDR_FTP_USER_NAME = "B";
    const CDR_FTP_PASSWORD = "C";
    const CDR_FTP_FILE_PATH = "D";
    // =======================================================================================================================
    
    // [╰参ㄏノTABLE嘿砞﹚] ====================================================================================================
    const TABLE_SYS_TABLE_MAPPING = "sys_table_mapping";
    const TABLE_SYS_KPI_FORMULA = "kpi_formula_setting";
    const TABLE_SYS_CONFIG_SETTING = "sys_config_setting";
    // =======================================================================================================================
    
    // [╰参闽把计砞﹚] =========================================================================================================
    const ORDER_BY_FOR_SYS_TABLE_MAPPING = "mapping_type, table_name, order_num asc";   // 砞﹚琩тSYS_TABLE_MAPPING逼よΑ
    const DEFAULT_CONDITION = "1=1";                // 箇砞WHERE兵ン
    const DEFAULT_ORDER_BY = "1";                   // 箇砞ORDER BYよΑ
    const DEFAULT_LIMIT = "";                       // 箇砞LIMIT兵ン
    const QUERY_ALL = "*";                          // Query allボΑ
    const FIELD_TABLE_NAME = "table_name";          // 盽计﹚竡
    const FIELD_ID = "id";                          // 盽计﹚竡
    const FIELD_ORI_NAME = "ori_name";              // 盽计﹚竡
    const FIELD_TARGET_NAME = "target_name";        // 盽计﹚竡
    const FIELD_ORDER_NUM = "order_num";            // 盽计﹚竡
    const FIELD_DATA_TYPE = "data_type";            // 盽计﹚竡
    const FIELD_HOST_NAME = "hostname";             // 盽计﹚竡
    const FIELD_MAPPING_TYPE = "mapping_type";      // 盽计﹚竡
    const FIELD_CATELOG = "catelog";                // 盽计﹚竡
    const FIELD_KPI_NAME = "kpi_name";              // 盽计﹚竡
    const FIELD_KPI_FORMULA = "kpi_formula";        // 盽计﹚竡
    const FIELD_SETTING_NAME = "setting_name";      // 盽计﹚竡
    const FIELD_SETTING_VALUE = "setting_value";    // 盽计﹚竡
    const FIELD_HOSTNAME = "hostname";              // 盽计﹚竡
    const FIELD_EPOCHTIME = "epochtime";            // 盽计﹚竡
    const FIELD_LOCALDATE = "localdate";            // 盽计﹚竡
    const FIELD_LOCALTIME = "localtime";            // 盽计﹚竡
    const FIELD_UPTIME = "uptime";                  // 盽计﹚竡
    
    const MAPPING_TYPE_TABLE = "TABLE";             // 盽计﹚竡
    const MAPPING_TYPE_FIELD = "FIELD";             // 盽计﹚竡
    
    const CONFIG_INTERVAL_STR = "INTERVAL_STR";     // 盽计﹚竡
    const CONFIG_INTERVAL = "INTERVAL";             // 盽计﹚竡
    // =======================================================================================================================