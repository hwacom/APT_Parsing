<?php
namespace Hwacom\APT_Parsing\utils;

use RecursiveIteratorIterator;
use RecursiveDirectoryIterator;

require_once dirname(__FILE__).'/../env/Config.inc';

class FileUtils {
    
    public function __construct() {
        
    }
    
    public function __destruct() {
        
    }
    
    public function getLocalFile() {
        $files = array();
        
        foreach (new RecursiveIteratorIterator(new RecursiveDirectoryIterator(PARSING_FILE_lOCAL_PATH)) as $filepath)
        {
            // filter out "." and ".."
            if ($filepath->isDir()) continue;
            
            $filename = $filepath->getFilename();
            $old_path = str_replace("\\", "/", $filepath);
            
            // 將檔案剪下到 Parsing system 處理資料夾
            $new_path = PARSING_FILE_PROCESS_PATH . $filename;
            $file_moved = rename($old_path, $new_path);
            
            if ($file_moved) {
                array_push($files, $new_path);
            }
        }
        
        return $files;
    }
    
    public function getLocalCDRFile() {
        $files = array();
        
        foreach (new RecursiveIteratorIterator(new RecursiveDirectoryIterator(CDR_DECODE_FILE_LOCAL_PATH)) as $filepath)
        {
            // filter out "." and ".."
            if ($filepath->isDir()) continue;
            
            $filename = $filepath->getFilename();
            $old_path = str_replace("\\", "/", $filepath);
            
            // 將檔案剪下到 CDR Parsing 處理資料夾
            $new_path = CDR_DECODE_FILE_PROCESS_PATH . $filename;
            $file_moved = rename($old_path, $new_path);
            
            if ($file_moved) {
                array_push($files, $new_path);
            }
        }
        
        return $files;
    }
}
