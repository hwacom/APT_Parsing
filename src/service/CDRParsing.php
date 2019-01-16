<?php
/*
 ** �w��ѪR������cDR output�ɮסAŪ�����e & �g�JDB
 */
date_default_timezone_set("Asia/Taipei");
require __DIR__ . '/../../vendor/autoload.php';
require_once dirname(__FILE__).'/../../vendor/apache/log4php/src/main/php/Logger.php';
require_once dirname(__FILE__).'/../env/Config.inc';

use Hwacom\APT_Parsing\dao\DatabaseAccessObject;
use Hwacom\APT_Parsing\utils\FileUtils;

class CDRParsing
{
    private $logger = null;
    
    private $tag_mapping = null;
    
    private $start_time = null;
    private $end_time = null;
    
    public function __construct() {
        Logger::configure(dirname(__FILE__).'/../env/log4php_cdr_parsing.xml');
        $this->logger = Logger::getLogger('file');
        
        $this->logger->info( "---------------------------------------------------------------------------------------------------------------------------------------------" );
        $this->logger->info( ">>>>>  START [ " . date("Y-m-d H:i:sa") . " ]" );
        $this->logger->info( "---------------------------------------------------------------------------------------------------------------------------------------------" );
        $this->start_time = time();
        
        $this->tag_mapping = array();
    }
    
    
    public function __destruct() {
        unset($GLOBALS['tag_mapping']);
        
        $this->end_time = time();
        $spent_time = $this->end_time - $this->start_time;
        $this->logger->info( "---------------------------------------------------------------------------------------------------------------------------------------------" );
        $this->logger->info( "<<<<<  END [ " . date("Y-m-d H:i:sa") . " ] (����ɶ�: $spent_time ��) " );
        $this->logger->info( "---------------------------------------------------------------------------------------------------------------------------------------------" );
        
        $this->logger = null;
    }
    
    
    public function execute() {
        try {
            /*
             * Step 1. �T�{���L�w�ѽX��parsing�ɮסA�Y���h�N�����ʦ�process�ؿ�
             */
            $this->logger->info( "Step 1. �T�{���L�w�ѽX��parsing�ɮסA�Y���h�N�����ʦ�process�ؿ�" );
            $file_utils = new FileUtils();
            $file_paths = $file_utils->getLocalCDRFile();
            
            if (empty($file_paths)) {
                throw new Exception("No files need to parsing.");
            }
            
            $this->logger->info( "***** " . count($file_paths) . " ���ɮ׻ݳB�z *****" );
            
            /*
             * Step 2. ��l��DB�s�u
             */
            $this->logger->info( "Step 2. ��l��DB�s�u" );
            $DAO = new DatabaseAccessObject(MYSQL_ADDRESS, MYSQL_USER_NAME, MYSQL_PASSWORD, CDR_MYSQL_DB_NAME);
            
            /*
             * Step 2-1. �d�ߴ����� (SYS_TABLE_MAPPING : ����Ӫ�)
             */
            $this->logger->info( "Step 2-1. �d�ߴ����� (SYS_TABLE_MAPPING : ����Ӫ�)" );
            $dataset = $DAO->query(TABLE_SYS_TABLE_MAPPING, DEFAULT_CONDITION, DEFAULT_ORDER_BY, QUERY_ALL, DEFAULT_LIMIT);
            
            /*
             * Step 2-2. �ഫ��mapping���
             */
            $this->logger->info( "Step 2-2. �ഫ��mapping���" );
            $this->composeMappingMap($dataset);
            
            $idx = 0;
            foreach ($file_paths as $path) {
                $idx++;
                $tmp_arr = explode("/", $path);
                $filename = $tmp_arr[count($tmp_arr) - 1];
                
                $this->logger->info( "============================================================================================================================================" );
                $this->logger->info( "�ɮ�$idx : $path " );
                try {
                    /*
                     * Step 3-1. �i��parsing�@�~
                     */
                    $this->logger->info( "Step 3-1. �i�� parsing �@�~" );
                    $parsing_set = $this->doParsing($path);
                    
                    /*
                     * Step 3-3. �Nparsing & KPI���G�g�JDB
                     */
                    $this->logger->info( "Step 3-2. �N CDR parsing ���G�g�JDB" );
                    $this->insertData2DB($DAO, $parsing_set);
                    
                    /*
                     ** �B�z���\�h�N�ɮײ���SUCCESS��Ƨ�
                     */
                    $this->logger->info( "Step 3-3. �N�ɮײ��ʦ� success ��Ƨ�" );
                    $new_path = CDR_DECODE_FILE_PROCESS_SUCCESS_PATH . $filename;
                    rename($path, $new_path);
                    
                } catch (Exception $t) {
                    $this->logger->error( "Caught exception:  ".$t->getMessage() );
                    
                    /*
                     ** �B�z���ѫh�N�ɮײ���ERROR��Ƨ�
                     */
                    $this->logger->info( "Step 3-3. �N�ɮײ��ʦ� error ��Ƨ�" );
                    $new_path = CDR_DECODE_FILE_PROCESS_ERROR_PATH . $filename;
                    rename($path, $new_path);
                    continue;
                    
                }
                
                /*
                 * Step 3-4. ��l�ƥ����ܼơA�B�z�U�@���ɮ�
                 */
                $this->logger->info( "Step 3-4. ��l�ƥ����ܼ�" );
                $parsing_set = null;
            }
            
            $this->logger->info( "============================================================================================================================================" );
              
        } catch (Exception $t) {
            $this->logger->error( "Caught exception:  ".$t->getMessage() );
        }
        
        $this->logger->info( "Step 4. ���槹���A����귽" );
    }
    
    /**
     * *�զX��Ӫ�MAP for ���� parsing �g�J database �ɨϥ�
     * @param array $dataset
     */
    private function composeMappingMap($dataset) {
        foreach ($dataset as $row) {
            
            $tag_name = $row[FIELD_TAG_NAME];
            
            if (array_key_exists($tag_name, $this->tag_mapping)) {
                continue;
                
            } else {
                $settings = array();
                $settings[FIELD_PREFIX] = $row[FIELD_PREFIX];
                $settings[FIELD_IGNORE_FLAG] = $row[FIELD_IGNORE_FLAG];
                $settings[FIELD_BEGIN_INDEX] = $row[FIELD_BEGIN_INDEX];
                $settings[FIELD_END_INDEX] = $row[FIELD_END_INDEX];
                $settings[FIELD_BEGIN_SYMBOL] = $row[FIELD_BEGIN_SYMBOL];
                $settings[FIELD_END_SYMBOL] = $row[FIELD_END_SYMBOL];
                $settings[FIELD_TARGET_TABLE_NAME] = $row[FIELD_TARGET_TABLE_NAME];
                $settings[FIELD_TARGET_TABLE_FIELD] = $row[FIELD_TARGET_TABLE_FIELD];
                
                $this->tag_mapping[$tag_name] = $settings;
            }
        }
    }
    
    /**
     * *�i���ɮפ��e���R
     * @param array $dataset
     */
    private function doParsing($path) {
        $dataset = array();
        $line_data = array();
        
        // �j��Ū���ɮפ��e ======================================================================================
        $file = fopen($path, "r");
        if ($file !== false) {
            
            // ���R�ɮ׸��|���X�ɦW ============================================================================
            $path_slice = explode("/", $path);
            $file_name = $path_slice[count($path_slice)-1];
            
            while(!feof($file))
            {
                $line = fgets($file);
                
                // �h�e��ť�
                $line = trim($line);
                
                // �ťզ���L
                if (empty($line) || $line == "") {
                    continue;
                }
                
                $is_cdr_tag = stripos($line, TAG_NAME_CDR);
                $is_parsing_finish = stripos($line, PARSING_FINISH_LINE);
                
                if ($is_cdr_tag !== false || $is_parsing_finish !== false) {
                    if (!empty($line_data)) {
                        array_push($dataset, $line_data);
                        $line_data = array();   
                    }
                    
                    if ($is_cdr_tag !== false) {
                        // �p�G�OCDR��ơA�NCDR�ȶ�Jdata array
                        $str_slice = explode(TAG_NAME_CDR, $line);
                        
                        $setting = $this->tag_mapping[TAG_NAME_CDR];
                        $table_field_name = $setting[FIELD_TARGET_TABLE_FIELD];
                        $table_field_value = trim($str_slice[1]);
                        
                        $line_data[$table_field_name] = $table_field_value;
                        $line_data[FIELD_FILE_NAME] = $file_name;
                    }
                    
                } else {
                    // �j����O�_���ŦX�� tag_name
                    foreach ($this->tag_mapping as $tag_name => $settings) {
                        $matched = stripos($line, $tag_name);
                        
                        if ($matched === false) {
                            continue;
                            
                        } else {
                            if ($settings[FIELD_IGNORE_FLAG] === "Y") {  // �Y�O�]�w��������tag_name�h���L
                                break;
                                
                            } else {
                                $str_slice = explode($tag_name, $line);
                                
                                if (!empty($str_slice)) {
                                    if (array_key_exists($tag_name, $this->tag_mapping)) {
                                        $setting = $this->tag_mapping[$tag_name];
                                        $table_field_name = $setting[FIELD_TARGET_TABLE_FIELD];
                                        $table_field_value = trim($str_slice[1]);
                                        
                                        $line_data[$table_field_name] = $table_field_value;
                                        
                                    } else {
                                        $this->logger->warn("***** TAG name:[$tag_name] not exists in tag_mapping !!\n");
                                    }
                                }
                                 
                                break;
                            }
                        }
                    }
                }
            }
            
            fclose($file);
        }
        // =================================================================================================
        
        return $dataset;
    }
    
    /**
     * *�Nparsing���G�g�JDB
     * @param array $parsingSet
     */
    private function insertData2DB($DAO, $parsing_set = array()) {
        /*
         ** �g�J Parsing ���
         */
        $DAO = new DatabaseAccessObject(MYSQL_ADDRESS, MYSQL_USER_NAME, MYSQL_PASSWORD, CDR_MYSQL_DB_NAME);
        foreach ($parsing_set as $data) {
            $insert_table = TABLE_CDR_DETAIL;
            
            $data[FIELD_CREATE_DATE] = date("Ymd");
            $data[FIELD_CREATE_TIME] = date("His");
            
            //unset($data["Mac_Of_Ap"]);
            //unset($data["cdr_no"]);
            
            $DAO->insert($insert_table, $data);
            
            $set = $this->tag_mapping[TAG_NAME_CDR];
            $field = $set[FIELD_TARGET_TABLE_FIELD];
        }
    }
}