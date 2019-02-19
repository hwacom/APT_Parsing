<?php
    // [PM Parsing Database config] ==========================================================================================================
    const MYSQL_ADDRESS = "localhost";      // Host
    const MYSQL_USER_NAME = "root";         // 眀腹
    const MYSQL_PASSWORD = "changeme";      // 盞絏
    const MYSQL_DB_NAME = "apt_parsing";    // 戈畐嘿
    // ======================================================================================================================
    
    // [Parsing result XML file storage path] ================================================================================================
    const OUTPUT_XML_FILE_PATH = "/var/www/html/xml/";

    // [PM Parsing source file setting] ======================================================================================================
    const PARSING_FILE_lOCAL_PATH = "/root/epdg_henbgw/csv/";                       // 郎隔畖
    const PARSING_FILE_PROCESS_PATH = "/root/epdg_henbgw/process/";                 // 矪瞶い郎ヘ魁
    const PARSING_FILE_PROCESS_SUCCESS_PATH = "/root/epdg_henbgw/process/success/"; // 矪瞶Θ郎ヘ魁
    const PARSING_FILE_PROCESS_ERROR_PATH = "/root/epdg_henbgw/process/error/";     // 矪瞶ア毖郎ヘ魁
    
    /*
    const PARSING_FILE_lOCAL_PATH = "D:\\Work\\Hwacom_地筿\@CASE\\2018.09_ㄈび-ePDG_Parsing\\Files\\source\\";                       // 郎隔畖
    const PARSING_FILE_PROCESS_PATH = "D:\\Work\\Hwacom_地筿\@CASE\\2018.09_ㄈび-ePDG_Parsing\\Files\\process\\";                 // 矪瞶い郎ヘ魁
    const PARSING_FILE_PROCESS_SUCCESS_PATH = "D:\\Work\\Hwacom_地筿\@CASE\\2018.09_ㄈび-ePDG_Parsing\\Files\\process\\success\\"; // 矪瞶Θ郎ヘ魁
    const PARSING_FILE_PROCESS_ERROR_PATH = "D:\\Work\\Hwacom_地筿\@CASE\\2018.09_ㄈび-ePDG_Parsing\\Files\\process\\error\\";     // 矪瞶ア毖郎ヘ魁
    */
    
    const PARSING_FILE_IGNORE_ROW_COUNT = 1;                    // 砞﹚郎ず甧秨繷┛菠ぃ矪瞶︽计
    const PARSING_HOST_NAME_SPLIT_SYMBOLS = "_bulkstats";       // 砞﹚ノㄓ秆猂郎眔[HOST_NAME]㏕﹚泊 (ノ泊ち澄郎皚[0]=HOST_NAME)
    const END_OF_FILE = "EndOfFile";                            // 郎挡Ю泊
    const ABNORMAL_SYMBOLS = "N/A";                             // 砞﹚讽逆篈ゅτㄓ方郎ず甧狦皚いㄤ泊玥糶DBず甧锣传[ABNORMAL_SYMBOL_TRANS_TO]
    const ABNORMAL_SYMBOL_TRANS_TO = "";                        // 钡[ABNORMAL_SYMBOLS]砞﹚讽獶タ盽ず甧璶锣传ぐ或泊
    // =======================================================================================================================
    
    // [cDR Database config] ==========================================================================================================
    const CDR_MYSQL_DB_NAME = "cdr_parsing";    // 戈畐嘿
    // ======================================================================================================================
    
    // [cDR decode file setting] ======================================================================================================
    const CDR_DECODE_FILE_LOCAL_PATH = "/root/epdg_cDR/decoded_cdr_tmp/";              
    const CDR_DECODE_FILE_PROCESS_PATH = "/root/epdg_cDR/process/";
    const CDR_DECODE_FILE_PROCESS_SUCCESS_PATH = "/root/epdg_cDR/process/success/";
    const CDR_DECODE_FILE_PROCESS_ERROR_PATH = "/root/epdg_cDR/process/error/";
    // =======================================================================================================================
    
    
    // [System usage table name setting] ====================================================================================================
    const TABLE_SYS_TABLE_MAPPING = "sys_table_mapping";
    const TABLE_SYS_SUBTRACTION_MAPPING = "sys_subtraction_setting";
    const TABLE_SYS_KPI_FORMULA = "sys_kpi_formula_setting";
    const TABLE_SYS_CONFIG_SETTING = "sys_config_setting";
    const TABLE_SYS_SUMMARY_TABLE_LIST = "sys_summary_table_list";
    
    const TABLE_CDR_DETAIL = "cdr_detail";
    // =======================================================================================================================
    
    // [System usage env variables setting] =========================================================================================================
    const ORDER_BY_FOR_SYS_TABLE_MAPPING = "mapping_type, table_name, order_num asc";   // 砞﹚琩тSYS_TABLE_MAPPING逼よΑ
    const DEFAULT_CONDITION = "1=1";                // 箇砞WHERE兵ン
    const DEFAULT_ORDER_BY = "1";                   // 箇砞ORDER BYよΑ
    const DEFAULT_LIMIT = "";                       // 箇砞LIMIT兵ン
    const LIMIT_1_ROW = "LIMIT 1";                  // 眔1掸戈
    const QUERY_ALL = "*";                          // Query allボΑ
    const FIELD_TABLE_NAME = "table_name";          // 盽计﹚竡
    const FIELD_ID = "id";                          // 盽计﹚竡
    const FIELD_ORI_NAME = "ori_name";              // 盽计﹚竡
    const FIELD_TARGET_NAME = "target_name";        // 盽计﹚竡
    const FIELD_ORDER_NUM = "order_num";            // 盽计﹚竡
    const FIELD_DATA_TYPE = "data_type";            // 盽计﹚竡
    const FIELD_HOST_NAME = "hostname";             // 盽计﹚竡
    const FIELD_VPN_NAME = "vpnname";               // 盽计﹚竡
    const FIELD_VPN_ID = "vpnid";                   // 盽计﹚竡
    const FIELD_SERV_NAME = "servname";             // 盽计﹚竡
    const FIELD_SERV_ID = "servid";                 // 盽计﹚竡
    const FIELD_MAPPING_TYPE = "mapping_type";      // 盽计﹚竡
    const FIELD_CATELOG = "catelog";                // 盽计﹚竡
    const FIELD_KPI_NAME = "kpi_name";              // 盽计﹚竡
    const FIELD_KPI_FORMULA = "kpi_formula";        // 盽计﹚竡
    const FIELD_SETTING_NAME = "setting_name";      // 盽计﹚竡
    const FIELD_SETTING_VALUE = "setting_value";    // 盽计﹚竡
    const FIELD_HOSTNAME = "hostname";              // 盽计﹚竡
    const FIELD_TYPE = "type";                      // 盽计﹚竡
    const FIELD_SUB_TYPE = "sub_type";              // 盽计﹚竡
    const FIELD_CARD = "card";                      // 盽计﹚竡
    const FIELD_IPADDR = "ipaddr";                  // 盽计﹚竡
    const FIELD_CREATE_DATE = "create_date";        // 盽计﹚竡
    const FIELD_CREATE_TIME = "create_time";        // 盽计﹚竡
    const FIELD_LOCAL_TIME = "local_time";          // 盽计﹚竡
    const FIELD_FREQUENCY = "frequency";            // 盽计﹚竡
    const FIELD_INTERVAL_BEGIN = "interval_begin";  // 盽计﹚竡
    const FIELD_INTERVAL_END = "interval_end";      // 盽计﹚竡
    const FIELD_EPOCHTIME = "epochtime";            // 盽计﹚竡
    const FIELD_LOCALDATE = "localdate";            // 盽计﹚竡
    const FIELD_LOCALTIME = "localtime";            // 盽计﹚竡
    const FIELD_UPTIME = "uptime";                  // 盽计﹚竡
    const FIELD_NAME = "field_name";                // 盽计﹚竡
    const FIELD_TABLE_TYPE = "table_type";          // 盽计﹚竡
    const FIELD_AGGREGATION_TYPE = "aggregation_type";    // 盽计﹚竡
    
    // CDR
    const FIELD_TAG_NAME = "tag_name";
    const FIELD_PREFIX = "prefix";
    const FIELD_IGNORE_FLAG = "ignore_flag";
    const FIELD_BEGIN_INDEX = "begin_index";
    const FIELD_END_INDEX = "end_index";
    const FIELD_BEGIN_SYMBOL = "begin_symbol";
    const FIELD_END_SYMBOL = "end_symbol";
    const FIELD_TARGET_TABLE_NAME = "target_table_name";
    const FIELD_TARGET_TABLE_FIELD = "target_table_field";
    const FIELD_FILE_NAME = "file_name";
    
    const PARSING_FINISH_LINE = "parsing finished";
    
    const TAG_NAME_CDR = "CDR";
    
    const MAPPING_TYPE_TABLE = "TABLE";             // 盽计﹚竡
    const MAPPING_TYPE_FIELD = "FIELD";             // 盽计﹚竡
    
    const CONFIG_INTERVAL_STR = "INTERVAL_STR";     // 盽计﹚竡
    const CONFIG_INTERVAL = "INTERVAL";             // 盽计﹚竡
    // =======================================================================================================================
    
    // [ePDG summary 把计砞﹚] =========================================================================================================
    const EPDG_SUMMARY_TIME_INTERVAL = "60";        // 砞﹚Summary璸衡丁跋丁(虫: だ)
    // =======================================================================================================================