<?php
namespace Hwacom\APT_Parsing\utils;

use RecursiveIteratorIterator;
use RecursiveDirectoryIterator;

require_once 'env\Config.inc';

class FileUtils {
    
    public function __construct() {
        
    }
    
    public function __destruct() {
        
    }
    
    public function getLocalFile() {
        $files = array();
        
        foreach (new RecursiveIteratorIterator(new RecursiveDirectoryIterator(PARSING_FILE_lOCAL_PATH)) as $filename)
        {
            // filter out "." and ".."
            if ($filename->isDir()) continue;
            
            $path = str_replace("\\", "/", $filename);
            array_push($files, $path);
        }
        
        return $files;
    }
    
    public function getRemoteFile() {
        
    }
}
