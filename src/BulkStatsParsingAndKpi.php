<?php
namespace Hwacom\APT_Parsing_Core;

use Hwacom\APT_Parsing_Core\dao\DatabaseAccessObject;
use Hwacom\APT_Parsing_Core\utils\FileUtils;
use Throwable;

require_once 'env\Config.inc';

class BulkStatsParsingAndKpi
{
    private $table_mapping = null;
    private $field_mapping = null;
    private $field_type_mapping = null;
    
    public function __construct() {
        $this->table_mapping = array();
        $this->field_mapping = array();
        $this->field_type_mapping = array();
    }
    
    public function __destruct() {
        unset($this->table_mapping);
        unset($this->field_mapping);
        unset($this->field_type_mapping);
    }
      
    public function execute() {
        try {
            
            /*
             * Step 1. ��l��DB�s�u
             */
            $DAO = new DatabaseAccessObject(MYSQL_ADDRESS, MYSQL_USER_NAME, MYSQL_PASSWORD, MYSQL_DB_NAME);
            
            /*
             * Step 2-1. �d�ߴ����� (SYS_TABLE_MAPPING)
             */
            $dataset = $DAO->query(TABLE_SYS_TABLE_MAPPING, DEFAULT_CONDITION, ORDER_BY_FOR_SYS_TABLE_MAPPING, QUERY_ALL, DEFAULT_LIMIT);
            
            /*
             * Step 2-2. �ഫ��mapping���
             */
            $this->composeMappingMap($dataset);
            
            /*
             * Step 2-3. �d�ߴ����� (SYS_KPI_FORMULA)
             */
            $dataset = $DAO->query(TABLE_SYS_KPI_FORMULA, DEFAULT_CONDITION, DEFAULT_ORDER_BY, QUERY_ALL, DEFAULT_LIMIT);
            
            /*
             * Step 2-4. �ഫ��mapping���
             */
            $this->composeKpiMap($dataset);
            
            /*
             * Step 3-1. ���o�nparsing���ɮ�
             */
            $file_utils = new FileUtils();
            $file_paths = $file_utils->getLocalFile();
            
            if (empty($file_paths)) {
                throw new \Exception("No files need to parsing.");
            }
            
            foreach ($file_paths as $path) {
                /*
                 * Step 3-2. �i��parsing�@�~
                 */
                $parsing_set = $this->doParsing($path);
                
                /*
                 * Step 3-3. �i��KPI�p��
                 */
                $kpi_set = $this->doKpiCalculate($parsing_set);
                
                /*
                 * Step 3-3. �Nparsing & KPI���G�g�JDB
                 */
                $this->insertData2DB($DAO, $parsing_set, $kpi_set);
            }
            
            /*
             * Step 4. ���槹���A����귽
             */
            
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
                    
                    for ($idx = 0; $idx < count($fields); $idx++) {
                        if (array_key_exists($table_name, $this->field_mapping)) {
                            $table_field = $this->field_mapping[$table_name];
                            
                            if (!empty($table_field[$idx])) {
                                $data[$table_field[$idx]] = $this->checkAbnormalDataContent($table_name, $idx, $fields[$idx]);
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
        
        //print_r($dataset);
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
    private function doKpiCalculate($dataset) {
        
    }
    
    /**
     * *�Nparsing�MKPI�p�⵲�G�g�JDB
     * @param array $parsingSet
     * @param array $kpiSet
     */
    private function insertData2DB($DAO, $parsing_set = array(), $kpi_set = array()) {
        foreach ($parsing_set as $data) {
            $insert_table = $this->table_mapping[$data[FIELD_TABLE_NAME]];
            
            unset($data[FIELD_TABLE_NAME]);
            
            $data_array = $data;
            $DAO->insert($insert_table, $data_array);
        }
    }
}
