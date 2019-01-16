<?php
date_default_timezone_set("Asia/Taipei");
require __DIR__ . '/../../vendor/autoload.php';
require_once dirname(__FILE__).'/../../vendor/apache/log4php/src/main/php/Logger.php';
require_once dirname(__FILE__).'/../env/Config.inc';

use Hwacom\APT_Parsing\dao\DatabaseAccessObject;
use Hwacom\APT_Parsing\utils\EvalMath;

class EPDGSummary
{
    private $logger = null;
    
    private $interval;
    private $begin_date;
    private $begin_time;
    private $end_date;
    private $end_time;
    private $cal_time_interval;
    private $cal_table_list;
    private $mapping_table;
    private $mapping_field;
    private $raw_data_mapping;
    private $kpi_mapping;
    
    private $start_timer = null;
    private $end_timer = null;
    
    public function __construct() {
        Logger::configure(dirname(__FILE__).'/../env/log4php_summary.xml');
        $this->logger = Logger::getLogger('file');
        
        $this->logger->info( "---------------------------------------------------------------------------------------------------------------------------------------------" );
        $this->logger->info( ">>>>>  START [ " . date("Y-m-d H:i:sa") . " ]" );
        $this->logger->info( "---------------------------------------------------------------------------------------------------------------------------------------------" );
        $this->start_timer = time();
        
        $this->cal_time_interval = EPDG_SUMMARY_TIME_INTERVAL;
        
        //TODO
        /* 測試用指定日期時間
        $date_yyyymmmdd = "2019-01-08";
        $date_hh24miss = "12:00:00";
        */
        
        $this->cal_table_list = array();
        $this->mapping_table = array();
        $this->mapping_field = array();
        $this->raw_data_mapping = array();
        $this->kpi_mapping = array();
    }
    
    public function __destruct() {
        unset($GLOBALS['$begin_date']);
        unset($GLOBALS['$begin_time']);
        unset($GLOBALS['$end_date']);
        unset($GLOBALS['$end_time']);
        unset($GLOBALS['cal_time_interval']);
        unset($GLOBALS['$cal_table_list']);
        unset($GLOBALS['$mapping_table']);
        unset($GLOBALS['$mapping_field']);
        unset($GLOBALS['$raw_data_mapping']);
        unset($GLOBALS['$kpi_mapping']);
        
        $this->end_timer = time();
        $spent_time = $this->end_timer - $this->start_timer;
        $this->logger->info( "---------------------------------------------------------------------------------------------------------------------------------------------" );
        $this->logger->info( "<<<<<  END [ " . date("Y-m-d H:i:sa") . " ] (執行時間: $spent_time 秒) " );
        $this->logger->info( "---------------------------------------------------------------------------------------------------------------------------------------------" );
     
        $this->logger = null;
    }
    
    public function execute($interval) {
        try {
            //TODO
            $this->interval = $interval;
            
            $date_yyyymmmdd = date("Y-m-d");
            $date_hh24miss = date("H:i:s");
            
            $dis_count = $this->interval;
            
            $date = date_create("$date_yyyymmmdd $date_hh24miss",timezone_open("Asia/Taipei"));
            if ($dis_count == "DAY") {
                date_add($date, date_interval_create_from_date_string("-1 days"));
                $this->begin_date = $date->format("Ymd");
                $this->begin_time = "000000";
                
            } elseif ($dis_count == "HOUR") {
                date_add($date, date_interval_create_from_date_string("-1 hours"));
                $this->begin_date = $date->format("Ymd");
                $this->begin_time = $date->format("H") . "0000";
                
            } else {
                date_add($date, date_interval_create_from_date_string("-$dis_count minutes"));
                $this->begin_date = $date->format("Ymd");
                $this->begin_time = $date->format("His");
            }
            
            $current_date = date_create("$date_yyyymmmdd $date_hh24miss",timezone_open("Asia/Taipei"));
            if ($dis_count == "DAY") {
                /*
                 ** 計算區間 = 天(DAY)
                 ** e.g. 20190110 000000 ~ 20190110 235959
                 */
                date_add($current_date, date_interval_create_from_date_string("-1 days"));
                $this->end_date = $date->format("Ymd");
                $this->end_time = "235959";
                
            } elseif ($dis_count == "HOUR") {
                /*
                 ** 計算區間 = 小時(HOUR)
                 ** e.g. 20190111 160000 ~ 20190111 165959
                 */
                date_add($current_date, date_interval_create_from_date_string("-1 hours"));
                $this->end_date = $date->format("Ymd");
                $this->end_time = $date->format("H") . "5959";
                
            } else {
                /*
                 ** 計算區間 = 自訂(分)
                 */
                $this->end_date = $current_date->format("Ymd");
                $this->end_time = $current_date->format("His");
            }
            
            $this->logger->info("計算資料日期時間區間: ".$this->begin_date." ".$this->begin_time." ~ ".$this->end_date." ".$this->end_time);
            
            $this->logger->info( "Step 1. 初始化DB連線" );
            $DAO = new DatabaseAccessObject(MYSQL_ADDRESS, MYSQL_USER_NAME, MYSQL_PASSWORD, MYSQL_DB_NAME);
            
            /*
             * Step 2-1. 查詢期初資料 (SYS_SUMMARY_TABLE_LIST : 需要計算SUMMARY的TABLE清單)
             */
            $this->logger->info( "Step 2-1. 查詢期初資料 (SYS_SUMMARY_TABLE_LIST : 需要計算SUMMARY的TABLE清單)" );
            $dataset = $DAO->query(TABLE_SYS_SUMMARY_TABLE_LIST, DEFAULT_CONDITION, DEFAULT_ORDER_BY, QUERY_ALL, DEFAULT_LIMIT);
            
            /*
             * Step 2-2. 轉換成mapping資料
             */
            $this->logger->info( "Step 2-2. 轉換成mapping資料" );
            $this->composeMappingMap($dataset);
            
            /*
             * Step 2-3. 查詢期初資料 (SYS_TABLE_MAPPING : 取得RAW_DATA table欄位設定的Summary方式)
             */
            $this->logger->info( "Step 2-3. 查詢期初資料 (SYS_TABLE_MAPPING : 取得RAW_DATA table欄位設定的Summary方式)" );
            $mapping_table = $DAO->query(TABLE_SYS_TABLE_MAPPING, "`mapping_type` = 'TABLE'", DEFAULT_ORDER_BY, QUERY_ALL, DEFAULT_LIMIT);
            $mapping_field = $DAO->query(TABLE_SYS_TABLE_MAPPING, "`mapping_type` = 'FIELD'", DEFAULT_ORDER_BY, QUERY_ALL, DEFAULT_LIMIT);
            
            /*
             * Step 2-4. 轉換成RAW_DATA_TABLE mapping資料
             */
            $this->logger->info( "Step 2-4. 轉換成RAW_DATA_TABLE mapping資料" );
            $this->composeRawDataMappingMap($mapping_table, $mapping_field);
            
            /*
             * Step 2-5. 查詢期初資料 (KPI_FORMULA_SETTING : 取得KPI table欄位設定的Summary方式)
             */
            $this->logger->info( "Step 2-5. 查詢期初資料 (KPI_FORMULA_SETTING : 取得KPI table欄位設定的Summary方式)" );
            $dataset = $DAO->query(TABLE_SYS_KPI_FORMULA, DEFAULT_CONDITION, DEFAULT_ORDER_BY, QUERY_ALL, DEFAULT_LIMIT);
            
            /*
             * Step 2-6. 轉換成KPI_TABLE mapping資料
             */
            $this->logger->info( "Step 2-6. 轉換成KPI_TABLE mapping資料" );
            $this->composeKpiMappingMap($dataset);
            
            if (empty($this->cal_table_list)) {
                $this->logger->info( "***未設定要計算SUMMARY的TABLE (SYS_SUMMARY_TABLE_LIST)" );
                
            } else {
                /*
                 * Step 3. 迴圈計算各TABLE的SUMMARY資訊
                 */
                $this->logger->info( "Step 3. 迴圈計算各TABLE的SUMMARY資訊" );
                $round = 1;
                foreach ($this->cal_table_list as $table_name => $table_type) {
                    try {
                        $this->calculateSummary($DAO, $table_name, $table_type, $round);
                        
                    } catch (Exception $t) {
                        $this->logger->error( "Caught exception:  ".$t->getMessage() );
                    }
                    $round++;
                }
            }
            
        } catch (Exception $t) {
            $this->logger->error( "Caught exception:  ".$t->getMessage() );
            
        }
    }
    
    private function calculateSummary($DAO, $table_name, $table_type, $round) {
        /*
         ** Step 3-2. 查詢符合區間內的所有資料
         */
        //$this->logger->info( "============================================================================================================================================" );
        //$this->logger->info( "TABLE_NAME$this->logger->info(able_name" );
        //$this->logger->info( "Step 3-2. 查詢符合區間內的所有資料" );
        /*
        $date_yyyymmmdd = date("Ymd");
        $date_hh24miss = date("His");
        */
        $condition = "(`localdate` >= $this->begin_date AND `localdate` <= $this->end_date) AND (`localtime` >= $this->begin_time AND `localtime` <= $this->end_time)";
        
        $data = $DAO->query($table_name, $condition, DEFAULT_ORDER_BY, QUERY_ALL, DEFAULT_LIMIT);
        
        if (!empty($data)) {
            /*
             ** Step 3-3. 區間內有資料，依TABLE類型，取得對應的TABLE欄位Summary設定做計算
             */
            //$this->logger->info( "Step 3-3. 區間內有資料，依TABLE類型，取得對應的TABLE欄位Summary設定做計算" );
            
            if ($table_type === "RAW") {
                $setting_map = $this->raw_data_mapping;
                
            } else if ($table_type === "KPI") {
                $setting_map = $this->kpi_mapping;
            }
            
            if (array_key_exists(strtoupper($table_name), $setting_map)) {
                $setting = $setting_map[strtoupper($table_name)];
                
                if ($table_type === "RAW") {
                    /*
                     ** 組合SUMMARY計算分群Key欄位
                     */
                    $key_fields = "";
                    foreach ($setting as $field_name => $aggregation_type) {
                        if ($aggregation_type === "[KEY]") {
                            $key_fields = $key_fields . "[" . $field_name . "]@~";
                        }
                    }
                }
                
                $summary = array();
                
                //$this->logger->info( "Step 3-4. 計算各欄位SUMMARY值" );
                
                $first_row = true;
                foreach ($data as $row) {
                    if ($first_row) {
                        $frequency = $this->interval;
                        
                        $first_row = false;
                    }
                    
                    if ($table_type === "RAW") {
                        /*
                         ** 計算RAW_DATA的SUMMARY，需要再根據Key欄位做SUMMARY分組
                         *  (e.g. vpnid / vpnname / card ...)
                         */
                        $key = $this->composeSummaryKey($row, $key_fields);
                        
                    } else if ($table_type === "KPI") {
                        /*
                         ** KPI SUMMARY不須再分組
                         */
                        $key = $row[FIELD_HOST_NAME];
                    }
                    
                    if (!array_key_exists($key, $summary)) {
                        $summary[$key] = array();
                    }
                    
                    $summary_value = $summary[$key];
                    
                    foreach ($setting as $field_name => $aggregation_type) {
                        if ($aggregation_type == "[KEY]") {
                            continue;
                            
                        } else if ($aggregation_type == "[VALUE]") {
                            $summary_value[$field_name] = $row[$field_name];
                            continue;
                        }
                        
                        // SUM
                        $summary_value = $this->doCalculate($row, $summary_value, $field_name, "_sum");
                        
                        // AVG
                        $summary_value = $this->doCalculate($row, $summary_value, $field_name, "_avg");
                        
                        // MAX
                        $summary_value = $this->doCalculate($row, $summary_value, $field_name, "_max");
                        
                        // MIN
                        $summary_value = $this->doCalculate($row, $summary_value, $field_name, "_min");
                    }
                    
                    $summary[$key] = $summary_value;
                }
                
                /*
                 ** 全部DATA計算完成後，將計算後的SUMMAY結果寫入DB
                 */
                //$this->logger->info( "Step 3-5. 計算完成，將計算後的SUMMAY結果寫入DB" );
                $final_table = $table_name . "_summary";
                
                foreach ($summary as $group_key => $value_array) {
                    if ($table_type === "RAW") {
                        $sql = "INSERT INTO `" . $final_table . "` ( `" . FIELD_CREATE_DATE . "`, `" . FIELD_CREATE_TIME . "`, `" . FIELD_FREQUENCY . "`, `" . FIELD_INTERVAL_BEGIN . "`, `" . FIELD_INTERVAL_END . "` ";
                        
                        $key_array = explode("@~", $key_fields);
                        foreach ($key_array as $field_name) {
                            $field_name = str_replace("[", "", $field_name);
                            $field_name = str_replace("]", "", $field_name);
                            
                            if (empty($field_name)) {
                                continue;
                            }
                            
                            $sql = $sql . ", `" . $field_name . "` ";
                        }
                        
                        foreach ($value_array as $field_name => $field_value) {
                            $sql = $sql . ", `" . $field_name . "` ";
                        }
                        
                        // INSERT ... VALUES >>>
                        $create_date = date("Ymd");
                        $create_time = date("His");
                        $sql = $sql . " ) VALUES ( " . $create_date . ", " . $create_time . ", '" . $frequency . "', '" . ((string)$this->begin_date).((string)$this->begin_time) . "', '" . ((string)$this->end_date).((string)$this->end_time) . "' ";
                        
                        $group_key_array = explode("@~", $group_key);
                        foreach ($group_key_array as $field_value) {
                            if (empty($field_value)) {
                                continue;
                            }
                            
                            $sql = $sql . ", '" . $field_value . "' ";
                        }
                        
                    } else if ($table_type === "KPI") {
                        $sql = "INSERT INTO `" . $final_table . "` ( `" . FIELD_HOSTNAME . "`, `". FIELD_CREATE_DATE . "`, `" 
                                    . FIELD_CREATE_TIME . "`, `" . FIELD_FREQUENCY . "`, `" . FIELD_INTERVAL_BEGIN . "`, `" . FIELD_INTERVAL_END . "` ";
                        
                        foreach ($value_array as $field_name => $field_value) {
                            $sql = $sql . ", `" . $field_name . "` ";
                        }
                        
                        // INSERT ... VALUES >>>
                        $create_date = date("Ymd");
                        $create_time = date("His");
                        $sql = $sql . " ) VALUES ( '" . $group_key . "', " . $create_date . ", " . $create_time . ", '" . $frequency . "', '" . ((string)$this->begin_date).((string)$this->begin_time) . "', '" . ((string)$this->end_date).((string)$this->end_time) . "' ";
                    }
                    
                    foreach ($value_array as $field_name => $field_value) {
                        if (strpos($field_value, "/") != false) {
                            $eval_math = new EvalMath();
                            $field_value = $eval_math->evaluate($field_value);
                        }
                        
                        if (is_numeric($field_value)) {
                            $sql = $sql . ", " . $field_value . " ";
                            
                        } else {
                            $sql = $sql . ", '" . $field_value . "' ";
                        }
                    }
                    
                    $sql = $sql . " ); ";
                    
                    //$this->logger->info( "sql: $sql " );
                    //echo "sql: $sql\n";
                    $DAO->insertBySQL($sql);
                }
                
                $this->logger->info( "[$round]: $table_name >>> Success!!" );
                
            } else {
                $this->logger->info( "[$round]: $table_name >>> Error!! <查找不到此TABLE的SUMMARY欄位設定> " );
                //$this->logger->info( "Step 3-3. 查找不到此TABLE的SUMMARY欄位設定，處理下一張TABLE" );
            }
            
        } else {
            $this->logger->info( "[$round]: $table_name >>> Skip!! <區間內無資料> " );
            //$this->logger->info( "Step 3-3. 區間內無資料，處理下一張TABLE" );
        }
    }
    
    private function doCalculate($row, $summary_value, $field_name, $type) {
        $agg_field_name = $field_name . $type;
        
        if (!array_key_exists($agg_field_name, $summary_value) && array_key_exists($field_name, $row)) {
            /*
             ** 計算AVG必須所有資料先加總完後再除，因此過程中都先以公式的方式記錄。直到要 INSERT入DB前再計算公式得出數值
             *  e.g. 220/3
             */
            $summary_value[$agg_field_name] = ($type == "_avg") ? ($row[$field_name] . "/1") : $row[$field_name];
            
        } else if (array_key_exists($agg_field_name, $summary_value)) {
            $pre_value = $summary_value[$agg_field_name];
            $row_value = $row[$field_name];
            
            $new_value = $pre_value;
            
            switch ($type) {
                case "_sum":
                    $new_value = $pre_value + $row_value;
                    break;
                    
                case "_avg":
                    if (empty($pre_value)) {
                        $new_value = $row_value . "/1";
                        
                    } else {
                        $tmp_arr = explode("/", $pre_value);
                        $p_val = $tmp_arr[0];
                        $round = $tmp_arr[1];
                        
                        $new_value = ($p_val + $row_value) . "/" . ($round + 1);
                    }
                    break;
                    
                case "_max":
                    $new_value = ($pre_value < $row_value) ? $row_value : $pre_value;
                    break;
                    
                case "_min":
                    $new_value = ($pre_value < $row_value) ? $pre_value : $row_value;
                    break;
            }
            
            
            $summary_value[$agg_field_name] = $new_value;
        }
        
        return $summary_value;
    }
    
    private function composeSummaryKey($row, $key_fields) {
        $key_array = explode("@~", $key_fields);
        
        $ret_key = $key_fields;
        foreach ($key_array as $key_field) {
            if (empty($key_field)) {
                continue;
            }
            
            $field_name = str_replace("[", "", $key_field);
            $field_name = str_replace("]", "", $field_name);
            
            if (array_key_exists($field_name, $row)) {
                $ret_key = str_replace($key_field, $row[$field_name], $ret_key);
            }
        }
        
        return $ret_key;
    }
    
    private function composeMappingMap($dataset) {
        foreach ($dataset as $row) {
            $table_name = $row[FIELD_TABLE_NAME];
            $table_type = $row[FIELD_TABLE_TYPE];
            
            $this->cal_table_list[$table_name] = $table_type;
        }
    }
    
    private function composeRawDataMappingMap($mapping_table, $mapping_field) {
        foreach ($mapping_table as $row) {
            $table_ori_name = $row[FIELD_ORI_NAME];
            $table_target_name = $row[FIELD_TARGET_NAME];
            
            $this->mapping_table[$table_ori_name] = $table_target_name;
        }
        
        foreach ($mapping_field as $row) {
            $table_ori_name = $row[FIELD_TABLE_NAME];
            $field_name = $row[FIELD_TARGET_NAME];
            $aggregation_type = $row[FIELD_AGGREGATION_TYPE];
            
            if ($aggregation_type == null) {
                continue;
            }
            
            if (array_key_exists($table_ori_name, $this->mapping_table)) {
                $table_target_name = $this->mapping_table[$table_ori_name];
                $table_target_name = strtoupper($table_target_name);
                
                $field_array = null;
                if (array_key_exists($table_target_name, $this->raw_data_mapping)) {
                    $field_array = $this->raw_data_mapping[$table_target_name];
                    
                } else {
                    $field_array = array();
                }
                
                $field_array[$field_name] = $aggregation_type;
                
                /*
                 ** 資料結構:
                 *  Array {
                 *      [TABLE_NAME] = Array {
                 *          [FIELD_NAME_1] = [AGGREGATION_TYPE_1],
                 *          [FIELD_NAME_2] = [AGGREGATION_TYPE_2],
                 *          ...
                 *      },
                 *      ...
                 *  }
                 */
                $this->raw_data_mapping[$table_target_name] = $field_array;
            }
        }
    }
    
    private function composeKpiMappingMap($dataset) {
        foreach ($dataset as $row) {
            $table_name = $row[FIELD_TABLE_NAME];
            $table_name = strtoupper($table_name);
            
            $field_name = $row[FIELD_KPI_NAME];
            $aggregation_type = $row[FIELD_AGGREGATION_TYPE];
            
            $field_array = null;
            if (array_key_exists($table_name, $this->kpi_mapping)) {
                $field_array = $this->kpi_mapping[$table_name];
                
            } else {
                $field_array = array();
            }
            
            $field_array[$field_name] = $aggregation_type;
            
            /*
             ** 資料結構:
             *  Array {
             *      [TABLE_NAME] = Array {
             *          [FIELD_NAME_1] = [AGGREGATION_TYPE_1],
             *          [FIELD_NAME_2] = [AGGREGATION_TYPE_2],
             *          ...
             *      },
             *      ...
             *  }
             */
            $this->kpi_mapping[$table_name] = $field_array;
        }
    }
}