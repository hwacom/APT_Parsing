<?php
date_default_timezone_set("Asia/Taipei");
require __DIR__ . '/../../vendor/autoload.php';
require_once dirname(__FILE__).'/../../vendor/apache/log4php/src/main/php/Logger.php';
require_once dirname(__FILE__).'/../env/Config.inc';

use Hwacom\APT_Parsing\dao\DatabaseAccessObject;
use Hwacom\APT_Parsing\utils\FileUtils;
use Hwacom\APT_Parsing\utils\EvalMath;
use Hwacom\APT_Parsing\utils\GuidUtils;

class EPDGParsingRecalculate
{
    private $logger = null;
    
    private $table_mapping = null;
    private $field_mapping = null;
    private $field_type_mapping = null;
    private $table_uk_mapping = null;               //�O���C�i��UK���
    private $field_subtraction_mapping = null;      //�����ݭn�e��۴�ƭȪ����W��
    private $kpi_formula = null;
    private $config_map = null;
    private $value_map = null;              //������U�ѪR���ɮפ��e�A�C�����������ȡA�ΥH����p��KPI�ɨϥ�
    
    private $c_hostname = null;
    private $c_epochtime = null;
    private $c_localdate = null;
    private $c_localtime = null;
    private $c_uptime = null;
    
    private $start_time = null;
    private $end_time = null;
    
    private $batch_no = null;
    private $begin_date_time = null;
    private $end_date_time = null;
    
    public function __construct() {
        Logger::configure(dirname(__FILE__).'/../env/log4php_epdg_recalculate.xml');
        $this->logger = Logger::getLogger('file');
        
        $guid_utils = new GuidUtils();
        $this->batch_no = $guid_utils->getGuid();
        
        $this->logger->info( "---------------------------------------------------------------------------------------------------------------------------------------------" );
        $this->logger->info( ">>>>>  START [ " . date("Y-m-d H:i:sa") . " ] >> Batch no.: " . $this->batch_no );
        $this->logger->info( "---------------------------------------------------------------------------------------------------------------------------------------------" );
        $this->start_time = time();
        
        $this->table_mapping = array();
        $this->field_mapping = array();
        $this->field_type_mapping = array();
        $this->table_uk_mapping = array();
        $this->field_subtraction_mapping = array();
        $this->kpi_formula = array();
        $this->config_map = array();
        $this->value_map = array();
        
        $this->c_hostname = null;
        $this->c_epochtime = null;
        $this->c_localdate = null;
        $this->c_localtime = null;
        $this->c_uptime = null;
    }
    
    public function __destruct() {
        unset($GLOBALS['table_mapping']);
        unset($GLOBALS['field_mapping']);
        unset($GLOBALS['field_type_mapping']);
        unset($GLOBALS['table_uk_mapping']);
        unset($GLOBALS['field_subtraction_mapping']);
        unset($GLOBALS['kpi_formula']);
        unset($GLOBALS['config_map']);
        unset($GLOBALS['value_map']);
        unset($GLOBALS['c_hostname']);
        unset($GLOBALS['c_epochtime']);
        unset($GLOBALS['c_localdate']);
        unset($GLOBALS['c_localtime']);
        unset($GLOBALS['c_uptime']);
        
        $this->end_time = time();
        $spent_time = $this->end_time - $this->start_time;
        $this->logger->info( "---------------------------------------------------------------------------------------------------------------------------------------------" );
        $this->logger->info( "<<<<<  END [ " . date("Y-m-d H:i:sa") . " ] (����ɶ�: $spent_time ��)  >> Batch no.: " . $this->batch_no );
        $this->logger->info( "---------------------------------------------------------------------------------------------------------------------------------------------" );
        
        $this->logger = null;
    }
    
    public function execute($begin_time, $end_time) {
        try {
            $this->begin_date_time = date_create_from_format("Y-m-d H:i", $begin_time);
            $this->end_date_time = date_create_from_format("Y-m-d H:i", $end_time);
            echo "begin_time: $begin_time , end_time: $end_time " . PHP_EOL;
            print "begin_date_time: " . $this->begin_date_time->format('Y-m-d H:i:s') . ", end_date_time: " . $this->end_date_time->format('Y-m-d H:i:s') . PHP_EOL;
            
            /*
             * Step 1. �P�_��J������϶��A���o�ݭn���⪺�ɮ�
             */
            $this->logger->info( "Step 1. �P�_��J������϶��A���o�ݭn���⪺�ɮ�" );
            $file_utils = new FileUtils();
            $file_paths = $file_utils->getLocalFileBySpecifyInterval($this->begin_date_time, $this->end_date_time);
            
            if (empty($file_paths)) {
                throw new Exception("No files need to parsing.");
            }
            
            $this->logger->info( "***** " . count($file_paths) . " ���ɮ׻ݳB�z *****" );
            
            /*
             * Step 2. ��l��DB�s�u
             */
            $this->logger->info( "Step 2. ��l��DB�s�u" );
            $DAO = new DatabaseAccessObject(MYSQL_ADDRESS, MYSQL_USER_NAME, MYSQL_PASSWORD, MYSQL_DB_NAME);
            
            /*
             * Step 2-1. �d�ߴ����� (SYS_TABLE_MAPPING : ����Ӫ�)
             */
            $this->logger->info( "Step 2-1. �d�ߴ����� (SYS_TABLE_MAPPING : ����Ӫ�)" );
            $dataset = $DAO->query(TABLE_SYS_TABLE_MAPPING, DEFAULT_CONDITION, ORDER_BY_FOR_SYS_TABLE_MAPPING, QUERY_ALL, DEFAULT_LIMIT);
            
            /*
             * Step 2-2. �ഫ��mapping���
             */
            $this->logger->info( "Step 2-2. �ഫ��mapping���" );
            $this->composeMappingMap($dataset);
            $this->composeTableUkMap($dataset);
            
            /*
             * Step 2-3. �d�ߴ����� (SYS_SUBTRACTION_MAPPING : ����Ӫ�)
             */
            $this->logger->info( "Step 2-3. �d�ߴ����� (SYS_SUBTRACTION_MAPPING : ����Ӫ�)" );
            $dataset = $DAO->query(TABLE_SYS_SUBTRACTION_MAPPING, DEFAULT_CONDITION, DEFAULT_ORDER_BY, QUERY_ALL, DEFAULT_LIMIT);
            
            /*
             * Step 2-4. �ഫ��mapping���
             */
            $this->logger->info( "Step 2-4. �ഫ��mapping���" );
            $this->composeSubtractionMap($dataset);
            
            /*
             * Step 2-5. �d�ߴ����� (SYS_KPI_FORMULA : KPI�����]�w��)
             */
            $this->logger->info( "Step 2-5. �d�ߴ����� (SYS_KPI_FORMULA : KPI�����]�w��)" );
            $dataset = $DAO->query(TABLE_SYS_KPI_FORMULA, DEFAULT_CONDITION, DEFAULT_ORDER_BY, QUERY_ALL, DEFAULT_LIMIT);
            
            /*
             * Step 2-6. �ഫ��mapping���
             */
            $this->logger->info( "Step 2-6. �ഫ��mapping���" );
            $this->composeKpiMap($dataset);
            
            /*
             * Step 2-7. �d�߳]�w�� (SYS_CONFIG_SETTING : �t�ΰѼƳ]�w��)
             */
            $this->logger->info( "Step 2-7. �d�߳]�w�� (SYS_CONFIG_SETTING : �t�ΰѼƳ]�w��)" );
            $dataset = $DAO->query(TABLE_SYS_CONFIG_SETTING, DEFAULT_CONDITION, DEFAULT_ORDER_BY, QUERY_ALL, DEFAULT_LIMIT);
            
            /*
             * Step 2-8. �ഫ��mapping���
             */
            $this->logger->info( "Step 2-8. �ഫ��mapping���" );
            $this->composeConfigMap($dataset);
            
            $idx = 0;
            foreach ($file_paths as $path) {
                $idx++;
                
                //TODO Linux:/ Windows:\\
                $tmp_arr = explode("/", $path);
                $filename = $tmp_arr[count($tmp_arr) - 1];
                
                $this->logger->info( "============================================================================================================================================" );
                $this->logger->info( "�ɮ�$idx : $path " );
                try {
                    /*
                     * Step 3-1. �i��parsing�@�~
                     */
                    $this->logger->info( "Step 3-1. �i��parsing�@�~" );
                    $parsing_set = $this->doParsing($path);
                    //print_r($parsing_set);
                    
                    /*
                     * Step 3-2. �i��KPI�p��
                     */
                    $this->logger->info( "Step 3-2. �i��KPI�p��" );
                    $kpi_set = $this->doKpiCalculate();
                    //print_r($kpi_set);
                    
                    /*
                     * Step 3-3. �Nparsing & KPI���G�g�JDB
                     */
                    $this->logger->info( "Step 3-3. �Nparsing & KPI���G�g�JDB" );
                    $this->insertData2DB($DAO, $parsing_set, $kpi_set);
                    
                    /*
                     ** �B�z���\�h�N�ɮײ���SUCCESS��Ƨ�
                     */
                    $this->logger->info( "Step 3-4. �N�ɮײ��ʦ� success ��Ƨ�" );
                    $new_path = PARSING_FILE_PROCESS_SUCCESS_PATH . $filename;
                    rename($path, $new_path);
                    
                } catch (Exception $t) {
                    $this->logger->error( "Caught exception:  ".$t->getMessage() );
                    
                    /*
                     ** �B�z���ѫh�N�ɮײ���ERROR��Ƨ�
                     */
                    $this->logger->info( "Step 3-4. �N�ɮײ��ʦ� error ��Ƨ�" );
                    $new_path = PARSING_FILE_PROCESS_ERROR_PATH . $filename;
                    rename($path, $new_path);
                    continue;
                    
                }// finally {
                /*
                 * Step 3-4. ��l�ƥ����ܼơA�B�z�U�@���ɮ�
                 */
                $this->logger->info( "Step 3-4. ��l�ƥ����ܼ�" );
                unset($GLOBALS['value_map']);
                unset($GLOBALS['c_hostname']);
                unset($GLOBALS['c_epochtime']);
                unset($GLOBALS['c_localdate']);
                unset($GLOBALS['c_localtime']);
                unset($GLOBALS['c_uptime']);
                //}
            }
            
            $this->logger->info( "============================================================================================================================================" );
            
        } catch (Exception $t) {
            $this->logger->error( "Caught exception:  ".$t->getMessage() );
            
        } //finally {
        /*
         * Step 4. ���槹���A����귽
         */
        $this->logger->info( "Step 4. ���槹���A����귽" );
        //}
    }
    
    /**
     * *�զX��Ӫ�MAP for ���� parsing �g�J database �ɨϥ�
     * @param array $dataset
     */
    private function composeMappingMap($dataset) {
        foreach ($dataset as $row) {
            
            $mapping_type = $row[FIELD_MAPPING_TYPE];
            
            if ($mapping_type === MAPPING_TYPE_TABLE) {
                $this->table_mapping[$row[FIELD_ORI_NAME]] = $row[FIELD_TARGET_NAME];
                
            } else if ($mapping_type === MAPPING_TYPE_FIELD) {
                $table_name = $row[FIELD_TABLE_NAME];
                
                $table_array = array();
                $table_type_array = array();
                
                if (array_key_exists($table_name, $this->field_mapping)) {
                    $table_array = $this->field_mapping[$table_name];
                    $table_type_array = $this->field_type_mapping[$table_name];
                }
                
                $table_array[((int)$row[FIELD_ORDER_NUM])-1] = $row[FIELD_TARGET_NAME];
                $this->field_mapping[$table_name] = $table_array;
                
                $table_type_array[((int)$row[FIELD_ORDER_NUM])-1] = $row[FIELD_DATA_TYPE];
                $this->field_type_mapping[$table_name] = $table_type_array;
            }
        }
    }
    
    private function composeTableUkMap($dataset) {
        
        foreach ($dataset as $row) {
            
            $mapping_type = $row[FIELD_MAPPING_TYPE];
            
            if ($mapping_type === MAPPING_TYPE_FIELD) {
                
                $table_name = $row[FIELD_TABLE_NAME];
                
                if (array_key_exists($table_name, $this->table_uk_mapping)) {
                    $uk_field = $this->table_uk_mapping[$table_name];
                    
                } else {
                    $uk_field = array();
                }
                
                $target_field_name = $row[FIELD_TARGET_NAME];
                $aggregation_type = $row[FIELD_AGGREGATION_TYPE];
                
                if ($aggregation_type == "[KEY]") {
                    if (!in_array($target_field_name, $uk_field)) {
                        array_push($uk_field, $target_field_name);
                        
                        $this->table_uk_mapping[$table_name] = $uk_field;
                    }
                }
            }
        }
    }
    
    /**
     * *�զX�ݭn�e��ƭȬ۴���W�ٰѷӪ� for ����parsing�ɭp��ϥ�
     * @param array $dataset
     */
    private function composeSubtractionMap($dataset) {
        foreach ($dataset as $row) {
            $field_name = $row[FIELD_NAME];
            
            $this->field_subtraction_mapping[$field_name] = 1;
        }
    }
    
    /**
     * *�զXKPI������ for ����parsing�ɭp��ϥ�
     * @param array $dataset
     */
    private function composeKpiMap($dataset) {
        foreach ($dataset as $row) {
            $table_name = $row[FIELD_TABLE_NAME];
            $kpi_name = $row[FIELD_KPI_NAME];
            $kpi_formula = $row[FIELD_KPI_FORMULA];
            
            $this->kpi_formula[$table_name][$kpi_name] = $kpi_formula;
        }
    }
    
    /**
     * *�զX�]�w��
     * @param array $dataset
     */
    private function composeConfigMap($dataset) {
        foreach ($dataset as $row) {
            $setting_name = $row[FIELD_SETTING_NAME];
            $setting_value = $row[FIELD_SETTING_VALUE];
            
            $this->config_map[$setting_name] = $setting_value;
        }
    }
    
    /**
     * *�i���ɮפ��e���R
     * @param array $dataset
     */
    private function doParsing($path) {
        $dataset = array();
        
        // ���R�ɦW���X [HOST_NAME] ============================================================================
        //TODO Linux:/ Windows:\\
        $path_slice = explode("/", $path);
        $file_name = $path_slice[count($path_slice)-1];
        
        $hostname = 'N/A';
        
        if (strpos($file_name, PARSING_HOST_NAME_SPLIT_SYMBOLS)) {
            $tmp = explode(PARSING_HOST_NAME_SPLIT_SYMBOLS, $file_name);
            $hostname = $tmp[0];
        }
        /* php 5.3.3 ���䴩 const ARRAY
         foreach (PARSING_HOST_NAME_SPLIT_SYMBOLS as $symbol) {
         if (strpos($file_name, $symbol)) {
         $tmp = explode($symbol, $file_name);
         $hostname = $tmp[0];
         break;
         }
         }
         */
        // =================================================================================================
        
        // �j��Ū���ɮפ��e ======================================================================================
        $row_num = 0;
        $file = fopen($path, "r");
        if ($file !== false) {
            
            $table_array = array();
            $temp_table_array = array();
            
            while (($fields = fgetcsv($file, 0, ",")) !== false) {
                if (empty($fields)) {
                    continue;
                }
                
                $data = array();
                $temp_data = array();
                
                if ($row_num >= PARSING_FILE_IGNORE_ROW_COUNT) { //�ɮײĤ@�C���B�z
                    if (count($fields) <= 2 || $fields[0] === END_OF_FILE) {
                        // �ɮ׳̫�@����L
                        continue;
                    }
                    
                    // �ĤG�C��}�l�ѪR���e�ù�����target table���W��
                    $table_name = $fields[2];
                    
                    $this->c_hostname = $hostname;
                    $this->c_epochtime = $fields[3];
                    $this->c_localdate = $fields[4];
                    $this->c_localtime = $fields[5];
                    $this->c_uptime = $fields[6];
                    
                    /*
                     ** ����ɤ��N��Ƽg�Jtemp��Ƨ�
                     ** �����z�L�e���B�J���X���϶����ɮװ��B�z
                     */
                    //TODO
                    $last_record_set = null;
                    
                    for ($idx = 0; $idx < count($fields); $idx++) {
                        if (array_key_exists($table_name, $this->field_mapping)) {
                            $table_field_array = $this->field_mapping[$table_name];
                            
                            if (!empty($table_field_array[$idx])) {
                                $field_name = $table_field_array[$idx];
                                $field_value = $fields[$idx];
                                
                                $temp_data[$field_name] = $this->checkAbnormalDataContent($table_name, $idx, $field_value);
                                
                                if (!empty($last_record_set)) {
                                    /*
                                     ** �Y���e�@����ơA����e�B�z�����O�_���]�w�ݭn���ƭȬ۴�
                                     */
                                    $field_var = "%$field_name%";   //�ഫ��DB���]�a�����W�ٮ榡(�e��H%�]��)
                                    
                                    if (array_key_exists($field_var, $this->field_subtraction_mapping)) {
                                        /*
                                         ** �Y����즳�]�w�n���ƭȬ۴�A�h�N��eCSVŪ��������($field_value)�DB���e�@����������($last_record)
                                         */
                                        $last_record = $last_record_set[0];
                                        $last_value = $last_record[$field_name];
                                        
                                        if (empty($field_value)) {
                                            $field_value = 0;
                                        }
                                        //echo "table_name: $db_table_name , field_name: $field_name , field_value: $field_value , last_value: $last_value \n";
                                        $field_value -= $last_value;
                                        
                                        if ($field_value < 0) {
                                            // Y190223, �]�ƥi��]�����ҫ�ƭȪ�l�ơA�ɭP�p��ɷ|�o��t�ȡA���ر��p�U�N�g�J��UCSV�����ƭ�
                                            $field_value = $fields[$idx];
                                        }
                                    }
                                }
                                
                                $data[$field_name] = $this->checkAbnormalDataContent($table_name, $idx, $field_value);
                                $this->value_map[$field_name] = $this->checkAbnormalDataContent($table_name, $idx, $field_value);
                            }
                        }
                    }
                    
                    $data[FIELD_TABLE_NAME] = $table_name;
                    $data[FIELD_HOST_NAME] = $hostname;
                    
                    $temp_data[FIELD_TABLE_NAME] = $table_name."_temp";
                    $temp_data[FIELD_HOST_NAME] = $hostname;
                    
                    array_push($table_array, $data);
                    array_push($temp_table_array, $temp_data);
                }
                
                $row_num++;
            }
            
            $dataset["MAIN"] = $table_array;
            $dataset["TEMP"] = $temp_table_array;
        }
        // =================================================================================================
        
        fclose($file);
        
        return $dataset;
    }
    
    private function checkAbnormalDataContent($table_name, $idx, $content) {
        $field_type = $this->field_type_mapping[$table_name][$idx];
        
        if ($field_type === 'number') {
            $content = is_numeric($content) ? $content : 0;
            
        } else {
            if ($content === ABNORMAL_SYMBOLS) {
                $content = ABNORMAL_SYMBOL_TRANS_TO;
            }
            /* php 5.3.3 ���䴩 const ARRAY
             foreach (ABNORMAL_SYMBOLS as $symbol) {
             if ($content === $symbol) {
             $content = ABNORMAL_SYMBOL_TRANS_TO;
             break;
             }
             }
             */
        }
        
        return $content;
    }
    
    /**
     * *�p��KPI�ƭ�
     * @param array $dataset
     */
    private function doKpiCalculate() {
        $dataset = array();
        $eval_math = new EvalMath();
        
        foreach ($this->kpi_formula as $table_name => $kpi_array) {
            $data = array();
            
            foreach ($kpi_array as $kpi_name => $formula) {
                $symbol_count = substr_count($formula, "%");
                
                // �ˮ֤������c�O�_���T (������H%�]���B%�`����������)
                if ($symbol_count == 0 || $symbol_count % 2 != 0) {
                    $this->logger->info( "***** Formula format excepton >> formula: $formula " );
                    
                } else {
                    $part = explode("%", $formula);
                    
                    for ($i = 1; $i < count($part); $i+=2) {
                        $map_key = "$part[$i]";
                        if (!array_key_exists($map_key, $this->value_map)) {
                            /*
                             ** ���W�٭Y���s�b�� Template ���d�򤺫h���L (��������]�w���w�ư������s�b������)
                             ** �ȳB�z: card / port / system / epdg / henbgw-access / henbgw-network / diameter-auth / egtpc
                             */
                            //$this->logger->info( "***** Variable not found excepton >> variable: $map_key " );
                            continue;
                            
                        } else {
                            // �N�������������ƭ�
                            $value = $this->value_map[$map_key];
                            $formula = str_replace("%$map_key%", $value, $formula);
                            
                            if (substr_count($formula, $this->config_map[CONFIG_INTERVAL_STR]) > 0) {
                                $formula = str_replace($this->config_map[CONFIG_INTERVAL_STR], $this->config_map[CONFIG_INTERVAL], $formula);
                            }
                        }
                    }
                    
                    // �A���ˮִ����ƭȫ᪺�������e�O�_�٧t��% (�Ψӥ]�����ҨϥΪ��Ÿ�)
                    $match = preg_match("/%/", $formula);
                    
                    //echo "formula: $formula, match: $match\r\n";
                    if ($match == 0) {
                        // �I�sAPI�i�椽���p��
                        $kpi_value = $eval_math->evaluate($formula);
                        
                        $data[$kpi_name] = $kpi_value;
                    }
                }
            }
            
            // ��J�B�~�һ����
            $data[FIELD_TABLE_NAME] = $table_name;
            $data[FIELD_HOST_NAME] = $this->c_hostname;
            $data[FIELD_EPOCHTIME] = $this->c_epochtime;
            $data[FIELD_LOCALDATE] = $this->c_localdate;
            $data[FIELD_LOCALTIME] = $this->c_localtime;
            $data[FIELD_UPTIME] = $this->c_uptime;
            
            array_push($dataset, $data);
        }
        
        return $dataset;
    }
    
    /**
     * *�Nparsing�MKPI�p�⵲�G�g�JDB
     * @param array $parsingSet
     * @param array $kpiSet
     */
    private function insertData2DB($DAO, $parsing_set = array(), $kpi_set = array()) {
        /*
         ** ��s/�s�W Parsing ���
         */
        //TODO
        foreach ($parsing_set as $table_type => $table_array) {
            foreach ($table_array as $data) {
                $table_name = $data[FIELD_TABLE_NAME];
                
                $insert_table = $this->table_mapping[$table_name];
                unset($data[FIELD_TABLE_NAME]);
                
                $data_array = $data;
                $DAO->insert(strtolower($insert_table), $data_array);
            }
        }
        
        /*
         ** ��s/�s�W KPI ���
         */
        //TODO
        foreach ($kpi_set as $data) {
            $insert_table = strtolower($data[FIELD_TABLE_NAME]);
            
            unset($data[FIELD_TABLE_NAME]);
            
            $data_array = $data;
            $DAO->insert($insert_table, $data_array);
        }
    }
}