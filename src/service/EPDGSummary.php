<?php
namespace Hwacom\APT_Parsing\service;

use Hwacom\APT_Parsing\dao\DatabaseAccessObject;
use Hwacom\APT_Parsing\utils\EvalMath;
use Throwable;

require_once './env/Config.inc';

class EPDGSummary
{
    private $cal_time_interval;
    private $cal_table_list;
    private $mapping_table;
    private $mapping_field;
    private $raw_data_mapping;
    private $kpi_mapping;
    
    private $start_time = null;
    private $end_time = null;
    
    public function __construct() {
        echo "---------------------------------------------------------------------------------------------------------------------------------------------\n";
        echo ">>>>>  START [ " . date("Y-m-d H:i:sa") . " ]\n";
        echo "---------------------------------------------------------------------------------------------------------------------------------------------\n";
        $this->start_time = time();
        
        $this->cal_time_interval = EPDG_SUMMARY_TIME_INTERVAL;
        $this->cal_table_list = array();
        $this->mapping_table = array();
        $this->mapping_field = array();
        $this->raw_data_mapping = array();
        $this->kpi_mapping = array();
    }
    
    public function __destruct() {
        unset($GLOBALS['cal_time_interval']);
        unset($GLOBALS['$cal_table_list']);
        unset($GLOBALS['$mapping_table']);
        unset($GLOBALS['$mapping_field']);
        unset($GLOBALS['$raw_data_mapping']);
        unset($GLOBALS['$kpi_mapping']);
        
        $this->end_time = time();
        $spent_time = $this->end_time - $this->start_time;
        echo "---------------------------------------------------------------------------------------------------------------------------------------------\n";
        echo "<<<<<  END [ " . date("Y-m-d H:i:sa") . " ] (����ɶ�: $spent_time ��) \n";
        echo "---------------------------------------------------------------------------------------------------------------------------------------------\n";
     }
    
    public function execute() {
        try {
            
            echo "Step 1. ��l��DB�s�u\n";
            $DAO = new DatabaseAccessObject(MYSQL_ADDRESS, MYSQL_USER_NAME, MYSQL_PASSWORD, MYSQL_DB_NAME);
            
            /*
             * Step 2-1. �d�ߴ����� (SYS_SUMMARY_TABLE_LIST : �ݭn�p��SUMMARY��TABLE�M��)
             */
            echo "Step 2-1. �d�ߴ����� (SYS_SUMMARY_TABLE_LIST : �ݭn�p��SUMMARY��TABLE�M��)\n";
            $dataset = $DAO->query(TABLE_SYS_SUMMARY_TABLE_LIST, DEFAULT_CONDITION, DEFAULT_ORDER_BY, QUERY_ALL, DEFAULT_LIMIT);
            
            /*
             * Step 2-2. �ഫ��mapping���
             */
            echo "Step 2-2. �ഫ��mapping���\n";
            $this->composeMappingMap($dataset);
            
            /*
             * Step 2-3. �d�ߴ����� (SYS_TABLE_MAPPING : ���oRAW_DATA table���]�w��Summary�覡)
             */
            echo "Step 2-3. �d�ߴ����� (SYS_TABLE_MAPPING : ���oRAW_DATA table���]�w��Summary�覡)\n";
            $mapping_table = $DAO->query(TABLE_SYS_TABLE_MAPPING, "`mapping_type` = 'TABLE'", DEFAULT_ORDER_BY, QUERY_ALL, DEFAULT_LIMIT);
            $mapping_field = $DAO->query(TABLE_SYS_TABLE_MAPPING, "`mapping_type` = 'FIELD'", DEFAULT_ORDER_BY, QUERY_ALL, DEFAULT_LIMIT);
            
            /*
             * Step 2-4. �ഫ��RAW_DATA_TABLE mapping���
             */
            echo "Step 2-4. �ഫ��RAW_DATA_TABLE mapping���\n";
            $this->composeRawDataMappingMap($mapping_table, $mapping_field);
            
            /*
             * Step 2-5. �d�ߴ����� (KPI_FORMULA_SETTING : ���oKPI table���]�w��Summary�覡)
             */
            echo "Step 2-5. �d�ߴ����� (KPI_FORMULA_SETTING : ���oKPI table���]�w��Summary�覡)n";
            $dataset = $DAO->query(TABLE_SYS_KPI_FORMULA, DEFAULT_CONDITION, DEFAULT_ORDER_BY, QUERY_ALL, DEFAULT_LIMIT);
            
            /*
             * Step 2-6. �ഫ��KPI_TABLE mapping���
             */
            echo "Step 2-6. �ഫ��KPI_TABLE mapping���\n";
            $this->composeKpiMappingMap($dataset);
            
            if (empty($this->cal_table_list)) {
                echo "***���]�w�n�p��SUMMARY��TABLE (SYS_SUMMARY_TABLE_LIST)";
                
            } else {
                /*
                 * Step 3-1. �j��p��UTABLE��SUMMARY��T
                 */
                echo "Step 3-1. �j��p��UTABLE��SUMMARY��T\n";
                foreach ($this->cal_table_list as $table_name => $table_type) {
                    $this->calculateSummary($DAO, $table_name, $table_type);
                }
            }
            
        } catch (Throwable $t) {
            echo 'Caught exception: ',  $t->getMessage(), "\n";
            
        } finally {
            
        }
    }
    
    private function calculateSummary($DAO, $table_name, $table_type) {
        /*
         ** Step 3-2. �d�߲ŦX�϶������Ҧ����
         */
        echo "============================================================================================================================================\n";
        echo "TABLE_NAME: $table_name\n";
        echo "Step 3-2. �d�߲ŦX�϶������Ҧ����\n";
        /*
        $date_yyyymmmdd = date("Ymd");
        $date_hh24miss = date("His");
        */
        $date_yyyymmmdd = "2016-09-06";
        $date_hh24miss = "12:00:00";
        $dis_minutes = $this->cal_time_interval;
        
        $date = date_create("$date_yyyymmmdd $date_hh24miss",timezone_open("Asia/Taipei"));
        date_add($date, date_interval_create_from_date_string("-$dis_minutes minutes"));
        
        $begin_date = $date->format("Ymd");
        $begin_time = $date->format("His");
        
        $condition = "`localdate` = $begin_date AND `localtime` >= $begin_time";
        
        $data = $DAO->query($table_name, $condition, DEFAULT_ORDER_BY, QUERY_ALL, DEFAULT_LIMIT);
        
        //TODO
        if (!empty($data)) {
            /*
             ** Step 3-3. �϶�������ơA��TABLE�����A���o������TABLE���Summary�]�w���p��
             */
            echo "Step 3-3. �϶�������ơA��TABLE�����A���o������TABLE���Summary�]�w���p��\n";
            
            if ($table_type === "RAW") {
                $setting_map = $this->raw_data_mapping;
                
            } else if ($table_type === "KPI") {
                $setting_map = $this->kpi_mapping;
            }
            
            if (array_key_exists($table_name, $setting_map)) {
                $setting = $setting_map[$table_name];
                
                if ($table_type === "RAW") {
                    /*
                     ** �զXSUMMARY�p����sKey���
                     */
                    $key_fields = "";
                    foreach ($setting as $field_name => $aggregation_type) {
                        if ($aggregation_type === "[KEY]") {
                            $key_fields = $key_fields . "[" . $field_name . "]@~";
                        }
                    }
                }
                
                $summary = array();
                
                echo "Step 3-4. �p��U���SUMMARY��\n";
                
                $first_row = true;
                foreach ($data as $row) {
                    if ($first_row) {
                        $frequency = EPDG_SUMMARY_TIME_INTERVAL;
                        
                        $first_row = false;
                    }
                    
                    if ($table_type === "RAW") {
                        /*
                         ** �p��RAW_DATA��SUMMARY�A�ݭn�A�ھ�Key��찵SUMMARY����
                         *  (e.g. vpnid / vpnname / card ...)
                         */
                        $key = $this->composeSummaryKey($row, $key_fields);
                        
                    } else if ($table_type === "KPI") {
                        /*
                         ** KPI SUMMARY�����A����
                         */
                        $key = "KPI";
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
                        }
                        
                        // SUM
                        $summary_value = $this->doCalculate($row, $summary_value, $field_name, "_sum");
                        
                        // AVG
                        $summary_value = $this->doCalculate($row, $summary_value, $field_name, "_avg");
                        
                        // MAX
                        $summary_value = $this->doCalculate($row, $summary_value, $field_name, "_max");
                        
                        // MIN
                        $summary_value = $this->doCalculate($row, $summary_value, $field_name, "_min");
                        
                        /*
                        if (!array_key_exists($field_name, $summary_value) && array_key_exists($field_name, $row)) {
                            $summary_value[$field_name] = $row[$field_name];
                            
                        } else if (array_key_exists($field_name, $summary_value)) {
                            $pre_value = $summary_value[$field_name];
                            $row_value = $row[$field_name];
                            
                            $new_value = $pre_value;
                            
                            //SUM
                            $new_value = $pre_value + $row_value;
                            $summary_value[$field_name."_sum"] = $new_value;
                            
                            //AVG
                            $new_value = ($pre_value + $row_value) / 2;
                            $summary_value[$field_name."_avg"] = $new_value;
                            
                            //MAX
                            $new_value = ($pre_value < $row_value) ? $row_value : $pre_value;
                            $summary_value[$field_name."_max"] = $new_value;
                            
                            //MIN
                            $new_value = ($pre_value < $row_value) ? $pre_value : $row_value;
                            $summary_value[$field_name."_min"] = $new_value;
                            
                            
                            switch ($aggregation_type) {
                                case "SUM":
                                    $new_value = $pre_value + $row_value;
                                    $summary_value[$field_name."_sum"] = $new_value;
                                    
                                case "AVG":
                                    $new_value = ($pre_value + $row_value) / 2;
                                    $summary_value[$field_name."_avg"] = $new_value;
                                    
                                case "MAX":
                                    $new_value = ($pre_value < $row_value) ? $row_value : $pre_value;
                                    $summary_value[$field_name."_max"] = $new_value;
                                    
                                case "MIN":
                                    $new_value = ($pre_value < $row_value) ? $pre_value : $row_value;
                                    $summary_value[$field_name."_min"] = $new_value;
                            }
                            
                        }
                        */
                    }
                    
                    $summary[$key] = $summary_value;
                }
                
                /*
                 ** ����DATA�p�⧹����A�N�p��᪺SUMMAY���G�g�JDB
                 */
                echo "Step 3-5. �p�⧹���A�N�p��᪺SUMMAY���G�g�JDB\n";
                $final_table = $table_name . "_summary";
                
                foreach ($summary as $group_key => $value_array) {
                    if ($table_type === "RAW") {
                        $sql = "INSERT INTO `" . $final_table . "` ( `" . FIELD_CREATE_DATE . "`, `" . FIELD_CREATE_TIME . "`, `" . FIELD_FREQUENCY . "` ";
                        
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
                        $create_date = date("Ymd", time());
                        $create_time = date("His", time());
                        $sql = $sql . " ) VALUES ( " . $create_date . ", " . $create_time . ", '" . $frequency . "' ";
                        
                        $group_key_array = explode("@~", $group_key);
                        foreach ($group_key_array as $field_value) {
                            if (empty($field_value)) {
                                continue;
                            }
                            
                            $sql = $sql . ", '" . $field_value . "' ";
                        }
                        
                        foreach ($value_array as $field_name => $field_value) {
                            if (strpos($field_value, "/") != false) {
                                $eval_math = new EvalMath();
                                $field_value = $eval_math->evaluate($field_value);
                            } 
                            
                            $sql = $sql . ", " . $field_value . " ";
                        }
                        
                        $sql = $sql . " ); ";
                        
                        echo "sql: $sql \n";
                        
                        //$DAO->insertBySQL($sql);
                        
                    } else if ($table_type === "KPI") {
                        $sql = "INSERT INTO " . $final_table . " ( `" . FIELD_HOSTNAME . "`". FIELD_CREATE_TIME . "`, `" 
                                . FIELD_LOCAL_TIME . "`, `" . FIELD_FREQUENCY . "`, ";
                        
                    }
                }
                
            } else {
                echo "Step 3-3. �d�䤣�즹TABLE��SUMMARY���]�w�A�B�z�U�@�iTABLE\n";
            }
            
        } else {
            echo "Step 3-3. �϶����L��ơA�B�z�U�@�iTABLE\n";
        }
    }
    
    private function doCalculate($row, $summary_value, $field_name, $type) {
        $agg_field_name = $field_name . $type;
        
        if (!array_key_exists($agg_field_name, $summary_value) && array_key_exists($field_name, $row)) {
            /*
             ** �p��AVG�����Ҧ���ƥ��[�`����A���A�]���L�{�������H�������覡�O���C����n INSERT�JDB�e�A�p�⤽���o�X�ƭ�
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
                
                $field_array = null;
                if (array_key_exists($table_target_name, $this->raw_data_mapping)) {
                    $field_array = $this->raw_data_mapping[$table_target_name];
                    
                } else {
                    $field_array = array();
                }
                
                $field_array[$field_name] = $aggregation_type;
                
                /*
                 ** ��Ƶ��c:
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
             ** ��Ƶ��c:
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