<?php
namespace Hwacom\APT_Parsing;

use Hwacom\APT_Parsing\dao\DatabaseAccessObject;
use Hwacom\APT_Parsing\utils\FileUtils;
use Hwacom\APT_Parsing\utils\EvalMath;
use Throwable;

require_once 'env\Config.inc';

class BulkStatsParsingAndKpi
{
    private $table_mapping = null;
    private $field_mapping = null;
    private $field_type_mapping = null;
    private $kpi_formula = null;
    private $config_map = null;
    private $value_map = null;              //������U�ѪR���ɮפ��e�A�C�����������ȡA�ΥH����p��KPI�ɨϥ�
    
    private $c_hostname = null;
    private $c_epochtime = null;
    private $c_localdate = null;
    private $c_localtime = null;
    private $c_uptime = null;
    
    public function __construct() {
        $this->table_mapping = array();
        $this->field_mapping = array();
        $this->field_type_mapping = array();
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
        unset($GLOBALS['kpi_formula']);
        unset($GLOBALS['config_map']);
        unset($GLOBALS['value_map']);
        unset($GLOBALS['c_hostname']);
        unset($GLOBALS['c_epochtime']);
        unset($GLOBALS['c_localdate']);
        unset($GLOBALS['c_localtime']);
        unset($GLOBALS['c_uptime']);
    }
      
    public function execute() {
        try {
            
            /*
             * Step 1. ��l��DB�s�u
             */
            echo "Step 1. ��l��DB�s�u\n";
            $DAO = new DatabaseAccessObject(MYSQL_ADDRESS, MYSQL_USER_NAME, MYSQL_PASSWORD, MYSQL_DB_NAME);
            
            /*
             * Step 2-1. �d�ߴ����� (SYS_TABLE_MAPPING : ����Ӫ�)
             */
            echo "Step 2-1. �d�ߴ����� (SYS_TABLE_MAPPING : ����Ӫ�)\n";
            $dataset = $DAO->query(TABLE_SYS_TABLE_MAPPING, DEFAULT_CONDITION, ORDER_BY_FOR_SYS_TABLE_MAPPING, QUERY_ALL, DEFAULT_LIMIT);
            
            /*
             * Step 2-2. �ഫ��mapping���
             */
            echo "Step 2-2. �ഫ��mapping���\n";
            $this->composeMappingMap($dataset);
            
            /*
             * Step 2-3. �d�ߴ����� (SYS_KPI_FORMULA : KPI�����]�w��)
             */
            echo "Step 2-3. �d�ߴ����� (SYS_KPI_FORMULA : KPI�����]�w��)\n";
            $dataset = $DAO->query(TABLE_SYS_KPI_FORMULA, DEFAULT_CONDITION, DEFAULT_ORDER_BY, QUERY_ALL, DEFAULT_LIMIT);
            
            /*
             * Step 2-4. �ഫ��mapping���
             */
            echo "Step 2-4. �ഫ��mapping���\n";
            $this->composeKpiMap($dataset);
            
            /*
             * Step 2-5. �d�߳]�w�� (SYS_CONFIG_SETTING : �t�ΰѼƳ]�w��)
             */
            echo "Step 2-5. �d�߳]�w�� (SYS_CONFIG_SETTING : �t�ΰѼƳ]�w��)\n";
            $dataset = $DAO->query(TABLE_SYS_CONFIG_SETTING, DEFAULT_CONDITION, DEFAULT_ORDER_BY, QUERY_ALL, DEFAULT_LIMIT);
            
            /*
             * Step 2-6. �ഫ��mapping���
             */
            echo "Step 2-6. �ഫ��mapping���\n";
            $this->composeConfigMap($dataset);
            
            /*
             * Step 3-1. ���o�nparsing���ɮ�
             */
            echo "Step 3-1. ���o�nparsing���ɮ�\n";
            $file_utils = new FileUtils();
            $file_paths = $file_utils->getLocalFile();
            
            if (empty($file_paths)) {
                throw new \Exception("No files need to parsing.");
            }
            
            foreach ($file_paths as $path) {
                /*
                 * Step 3-2. �i��parsing�@�~
                 */
                echo "Step 3-2. �i��parsing�@�~\n";
                $parsing_set = $this->doParsing($path);
                
                /*
                 * Step 3-3. �i��KPI�p��
                 */
                echo "Step 3-3. �i��KPI�p��\n";
                $kpi_set = $this->doKpiCalculate();
                
                /*
                 * Step 3-4. �Nparsing & KPI���G�g�JDB
                 */
                echo "Step 3-4. �Nparsing & KPI���G�g�JDB\n";
                $this->insertData2DB($DAO, $parsing_set, $kpi_set);
                
                /*
                 * Step 3-5. ��l�ƥ����ܼơA�B�z�U�@���ɮ�
                 */
                echo "Step 3-5. ��l�ƥ����ܼ�\n";
                unset($GLOBALS['value_map']);
                unset($GLOBALS['c_hostname']);
                unset($GLOBALS['c_epochtime']);
                unset($GLOBALS['c_localdate']);
                unset($GLOBALS['c_localtime']);
                unset($GLOBALS['c_uptime']);
            }
            
            /*
             * Step 4. ���槹���A����귽
             */
            echo "Step 4. ���槹���A����귽\n";
            //unset($this->value_map);
            
        } catch (Throwable $t) {
            echo 'Caught exception: ',  $t->getMessage(), "\n";
        }
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
                
            } elseif ($mapping_type === MAPPING_TYPE_FIELD) {
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
        /*
        echo "[TABLE_MAPPING] ========================================================\n";
        print_r($this->table_mapping);
        echo "========================================================================\n";
        echo "[FIELD_MAPPING] ========================================================\n";
        print_r($this->field_mapping);
        echo "========================================================================\n";
        */
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
        $path_slice = explode("/", $path);
        $file_name = $path_slice[count($path_slice)-1];
        
        $hostname = 'N/A';
        foreach (PARSING_HOST_NAME_SPLIT_SYMBOLS as $symbol) {
            if (strpos($file_name, $symbol)) {
                $hostname = explode($symbol, $file_name)[0];
                break;
            }
        }
        // =================================================================================================
        
        // �j��Ū���ɮפ��e ======================================================================================
        $row_num = 0;
        $file = fopen($path, "r");
        if ($file !== false) {
            while (($fields = fgetcsv($file, 0, ",")) !== false) {
                if (empty($fields)) {
                    continue;
                }
                
                $data = array();
                
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
                    
                    for ($idx = 0; $idx < count($fields); $idx++) {
                        if (array_key_exists($table_name, $this->field_mapping)) {
                            $table_field = $this->field_mapping[$table_name];
                            
                            if (!empty($table_field[$idx])) {
                                $data[$table_field[$idx]] = $this->checkAbnormalDataContent($table_name, $idx, $fields[$idx]);
                                $this->value_map[$table_field[$idx]] = $this->checkAbnormalDataContent($table_name, $idx, $fields[$idx]);
                            }
                        }
                    }
                    
                    $data[FIELD_TABLE_NAME] = $table_name;
                    $data[FIELD_HOST_NAME] = $hostname;
                    
                    array_push($dataset, $data);
                }
                
                $row_num++;
            }
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
            foreach (ABNORMAL_SYMBOLS as $symbol) {
                if ($content === $symbol) {
                    $content = ABNORMAL_SYMBOL_TRANS_TO;
                    break;
                }
            }
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
                    echo "***** Formula format excepton >> formula: $formula \n";
                    
                } else {
                    $part = explode("%", $formula);
                    
                    for ($i = 1; $i < count($part); $i+=2) {
                        $map_key = "$part[$i]";
                        if (!array_key_exists($map_key, $this->value_map)) {
                            /*
                             * ���W�٭Y���s�b�� Template ���d�򤺫h���L (��������]�w���w�ư������s�b������)
                             * �ȳB�z: card / port / system / epdg / henbgw-access / henbgw-network / diameter-auth / egtpc
                             */
                            echo "***** Variable not found excepton >> variable: $map_key \n";
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
                    
                    if ($match == 0) {
                        // �I�sAPI�i�椽���p��
                        $kpi_value = $eval_math->evaluate($formula);
                    }
                    
                    $data[$kpi_name] = $kpi_value;
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
         * �g�J Parsing ���
         */
        foreach ($parsing_set as $data) {
            $insert_table = $this->table_mapping[$data[FIELD_TABLE_NAME]];
            
            unset($data[FIELD_TABLE_NAME]);
            
            $data_array = $data;
            $DAO->insert($insert_table, $data_array);
        }
        
        /*
         * �g�J KPI ���
         */
        foreach ($kpi_set as $data) {
            $insert_table = $data[FIELD_TABLE_NAME];
            
            unset($data[FIELD_TABLE_NAME]);
            
            $data_array = $data;
            $DAO->insert($insert_table, $data_array);
        }
    }
}
