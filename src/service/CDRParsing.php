<?php
/*
 ** 針對解析完成的cDR output檔案，讀取內容 & 寫入DB
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
        $this->logger->info( "<<<<<  END [ " . date("Y-m-d H:i:sa") . " ] (執行時間: $spent_time 秒) " );
        $this->logger->info( "---------------------------------------------------------------------------------------------------------------------------------------------" );
        
        $this->logger = null;
    }
    
    
    public function execute() {
        try {
            /*
             * Step 1. 確認有無已解碼待parsing檔案，若有則將之移動至process目錄
             */
            $this->logger->info( "Step 1. 確認有無已解碼待parsing檔案，若有則將之移動至process目錄" );
            $file_utils = new FileUtils();
            $file_paths = $file_utils->getLocalCDRFile();
            
            if (empty($file_paths)) {
                throw new Exception("No files need to parsing.");
            }
            
            $this->logger->info( "***** " . count($file_paths) . " 份檔案需處理 *****" );
            
            /*
             * Step 2. 初始化DB連線
             */
            $this->logger->info( "Step 2. 初始化DB連線" );
            $DAO = new DatabaseAccessObject(MYSQL_ADDRESS, MYSQL_USER_NAME, MYSQL_PASSWORD, CDR_MYSQL_DB_NAME);
            
            /*
             * Step 2-1. 查詢期初資料 (SYS_TABLE_MAPPING : 欄位對照表)
             */
            $this->logger->info( "Step 2-1. 查詢期初資料 (SYS_TABLE_MAPPING : 欄位對照表)" );
            $dataset = $DAO->query(TABLE_SYS_TABLE_MAPPING, DEFAULT_CONDITION, DEFAULT_ORDER_BY, QUERY_ALL, DEFAULT_LIMIT);
            
            /*
             * Step 2-2. 轉換成mapping資料
             */
            $this->logger->info( "Step 2-2. 轉換成mapping資料" );
            $this->composeMappingMap($dataset);
            
            $idx = 0;
            foreach ($file_paths as $path) {
                $idx++;
                $tmp_arr = explode("/", $path);
                $filename = $tmp_arr[count($tmp_arr) - 1];
                
                $this->logger->info( "============================================================================================================================================" );
                $this->logger->info( "檔案$idx : $path " );
                try {
                    /*
                     * Step 3-1. 進行parsing作業
                     */
                    $this->logger->info( "Step 3-1. 進行 parsing 作業" );
                    $parsing_set = $this->doParsing($path);
                    
                    /*
                     * Step 3-3. 將parsing & KPI結果寫入DB
                     */
                    $this->logger->info( "Step 3-2. 將 CDR parsing 結果寫入DB" );
                    $this->insertData2DB($DAO, $parsing_set);
                    
                    /*
                     ** 處理成功則將檔案移至SUCCESS資料夾
                     */
                    $this->logger->info( "Step 3-3. 將檔案移動至 success 資料夾" );
                    $new_path = CDR_DECODE_FILE_PROCESS_SUCCESS_PATH . $filename;
                    rename($path, $new_path);
                    
                } catch (Exception $t) {
                    $this->logger->error( "Caught exception:  ".$t->getMessage() );
                    
                    /*
                     ** 處理失敗則將檔案移至ERROR資料夾
                     */
                    $this->logger->info( "Step 3-3. 將檔案移動至 error 資料夾" );
                    $new_path = CDR_DECODE_FILE_PROCESS_ERROR_PATH . $filename;
                    rename($path, $new_path);
                    continue;
                    
                }
                
                /*
                 * Step 3-4. 初始化全域變數，處理下一個檔案
                 */
                $this->logger->info( "Step 3-4. 初始化全域變數" );
                $parsing_set = null;
            }
            
            $this->logger->info( "============================================================================================================================================" );
              
        } catch (Exception $t) {
            $this->logger->error( "Caught exception:  ".$t->getMessage() );
        }
        
        $this->logger->info( "Step 4. 執行完成，釋放資源" );
    }
    
    /**
     * *組合對照表MAP for 後續 parsing 寫入 database 時使用
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
     * *進行檔案內容分析
     * @param array $dataset
     */
    private function doParsing($path) {
        $dataset = array();
        $line_data = array();
        
        // 迴圈讀取檔案內容 ======================================================================================
        $file = fopen($path, "r");
        if ($file !== false) {
            
            // 分析檔案路徑取出檔名 ============================================================================
            $path_slice = explode("/", $path);
            $file_name = $path_slice[count($path_slice)-1];
            
            while(!feof($file))
            {
                $line = fgets($file);
                
                // 去前後空白
                $line = trim($line);
                
                // 空白行跳過
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
                        // 如果是CDR行數，將CDR值塞入data array
                        $str_slice = explode(TAG_NAME_CDR, $line);
                        
                        $setting = $this->tag_mapping[TAG_NAME_CDR];
                        $table_field_name = $setting[FIELD_TARGET_TABLE_FIELD];
                        $table_field_value = trim($str_slice[1]);
                        
                        $line_data[$table_field_name] = $table_field_value;
                        $line_data[FIELD_FILE_NAME] = $file_name;
                    }
                    
                } else {
                    // 迴圈比對是否有符合的 tag_name
                    foreach ($this->tag_mapping as $tag_name => $settings) {
                        $matched = stripos($line, $tag_name);
                        
                        if ($matched === false) {
                            continue;
                            
                        } else {
                            if ($settings[FIELD_IGNORE_FLAG] === "Y") {  // 若是設定為忽略的tag_name則跳過
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
     * *將parsing結果寫入DB
     * @param array $parsingSet
     */
    private function insertData2DB($DAO, $parsing_set = array()) {
        /*
         ** 寫入 Parsing 資料
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