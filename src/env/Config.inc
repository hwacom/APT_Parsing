<?php
    // [資料庫連線設定] ==========================================================================================================
    const MYSQL_ADDRESS = "localhost";      // Host
    const MYSQL_USER_NAME = "root";         // 帳號
    const MYSQL_PASSWORD = "root1234";      // 密碼
    const MYSQL_DB_NAME = "apt_parsing";    // 資料庫名稱
    // ======================================================================================================================

    // [Parsing 檔案設定] ======================================================================================================
    const PARSING_FILE_lOCAL_PATH = "D:/Work/Hwacom_華電/@CASE/2018.09_亞太-ePDG/Files/target/";    // 檔案路徑
    const PARSING_FILE_IGNORE_ROW_COUNT = 1;            // 設定檔案內容開頭忽略不處理的行數
    const PARSING_HOST_NAME_SPLIT_SYMBOLS = array(      // 設定用來解析檔名取得[HOST_NAME]的固定字眼 (用此字眼切割檔名後取陣列[0]=HOST_NAME)
        "_bulkstats"
    );
    const END_OF_FILE = "EndOfFile";                    // 檔案結尾字眼
    const ABNORMAL_SYMBOLS = array (                    // 設定當欄位型態為文字，而來源檔內容如果為陣列中其一的字眼，則寫入DB的內容轉換為[ABNORMAL_SYMBOL_TRANS_TO]的值
        "N/A"                                   
    );
    const ABNORMAL_SYMBOL_TRANS_TO = "";                // 接[ABNORMAL_SYMBOLS]，設定當非正常的內容要轉換為什麼字眼
    // =======================================================================================================================
    
    // [CDR檔案取得FTP連線設定] ===================================================================================================
    const CDR_FTP_HOST_ADDRESS = "A";
    const CDR_FTP_USER_NAME = "B";
    const CDR_FTP_PASSWORD = "C";
    const CDR_FTP_FILE_PATH = "D";
    // =======================================================================================================================
    
    // [系統使用TABLE名稱設定] ====================================================================================================
    const TABLE_SYS_TABLE_MAPPING = "sys_table_mapping";
    const TABLE_SYS_KPI_FORMULA = "sys_kpi_formula";
    // =======================================================================================================================
    
    // [系統相關參數設定] =========================================================================================================
    const ORDER_BY_FOR_SYS_TABLE_MAPPING = "mapping_type, table_name, order_num asc";   // 設定查找SYS_TABLE_MAPPING的排序方式
    const DEFAULT_CONDITION = "1=1";            // 預設的WHERE條件
    const DEFAULT_ORDER_BY = "1";               // 預設的ORDER BY方式
    const DEFAULT_LIMIT = "";                   // 預設的LIMIT條件
    const QUERY_ALL = "*";                      // Query all的表示式
    const FIELD_TABLE_NAME = "table_name";      // 常數定義
    const FIELD_ID = "id";                      // 常數定義
    const FIELD_ORI_NAME = "ori_name";          // 常數定義
    const FIELD_TARGET_NAME = "target_name";    // 常數定義
    const FIELD_ORDER_NUM = "order_num";        // 常數定義
    const FIELD_DATA_TYPE = "data_type";        // 常數定義
    const FIELD_HOST_NAME = "hostname";         // 常數定義
    const FIELD_MAPPING_TYPE = "mapping_type";  // 常數定義
    const MAPPING_TYPE_TABLE = "TABLE";         // 常數定義
    const MAPPING_TYPE_FIELD = "FIELD";         // 常數定義
    // =======================================================================================================================