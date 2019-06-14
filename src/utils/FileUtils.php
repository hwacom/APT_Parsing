<?php
namespace Hwacom\APT_Parsing\utils;

use RecursiveFilterIterator;
use RecursiveIterator;
use RecursiveIteratorIterator;
use RecursiveDirectoryIterator;

require_once dirname(__FILE__).'/../env/Config.inc';

class FileUtils {
    
    public function __construct() {
        
    }
    
    public function __destruct() {
        
    }
    
    /**
     **判斷資料夾路徑是否存在，不存在則建立
     * @param $dir_path
     */
    public function checkIfDirExistsOrCreate($dir_path) {
        if (!file_exists($dir_path) && !is_dir($dir_path)) {
            mkdir($dir_path, 0777, true);
        }
    }
    
    /**
     ** 取出符合日期範圍內的檔案路徑，放至Array內回傳
     * @param $files_array
     * @param $dir_path
     * @param $begin_date_time
     * @param $end_date_time
     * @return
     */
    public function chkMatchedFiles($files_array, $dir_path, $begin_date_time, $end_date_time) {
        //判斷資料夾是否存在 OR 創建
        $this->checkIfDirExistsOrCreate($dir_path);
        
        $directory = new RecursiveDirectoryIterator($dir_path);
        $filter = new FilesystemDateFilter($directory, $begin_date_time->getTimestamp(), $end_date_time->getTimestamp());
        
        foreach(new RecursiveIteratorIterator($filter) as $path) {
            array_push($files_array, $path->getPathname());
        }
        
        return $files_array;
    }
    
    /**
     **取得HeNBGW備份目錄下，符合日期區間內的所有檔案
     * @param $begin_time
     * @param $end_time
     */
    public function getLocalFileBySpecifyInterval($begin_date_time, $end_date_time) {
        $files = array();
        
        $begin_date_str = $begin_date_time->format("Y-m-d");
        $begin_time_str = $begin_date_time->format("H:i");
        
        if ($begin_time_str === "00:00") {
            //如果輸入的起始時間為「00:00」，則須往前推一天取得前一天最後一份檔案「23:55」
            $pre_date_time = date_create_from_format("Y-m-d H:i", $begin_date_str." 23:55");
            $pre_end_date_time = date_create_from_format("Y-m-d H:i", $begin_date_str." 00:00");
            date_sub($pre_date_time, date_interval_create_from_date_string('1 days'));
            
            $pre_date_str = $pre_date_time->format("Y-m-d");
            
            //先看SUCCESS資料夾
            $pre_date_dir = PARSING_FILE_PROCESS_SUCCESS_PATH . $pre_date_str . "\\";
            //echo "先看SUCCESS資料夾 >> $pre_date_dir".PHP_EOL;
            $files = $this->chkMatchedFiles($files, $pre_date_dir, $pre_date_time, $pre_end_date_time);
            
            //再看ERROR資料夾
            $pre_date_dir = PARSING_FILE_PROCESS_ERROR_PATH . $pre_date_str . "\\";
            //echo "先看ERROR資料夾 >> $pre_date_dir".PHP_EOL;
            $files = $this->chkMatchedFiles($files, $pre_date_dir, $pre_date_time, $pre_end_date_time);
            
            $begin_date_time = date_create_from_format("Y-m-d H:i", $begin_date_str." 00:00");
            
        } else {
            //Begin time 須往前推5分鐘，抓出前一次的數據才能做計算
            date_sub($begin_date_time, date_interval_create_from_date_string('5 minutes'));
        }
        
        //先看SUCCESS資料夾
        $begin_date_dir = PARSING_FILE_PROCESS_SUCCESS_PATH . $begin_date_str . "\\";
        //echo "先看SUCCESS資料夾 >> $begin_date_dir".PHP_EOL;
        $files = $this->chkMatchedFiles($files, $begin_date_dir, $begin_date_time, $end_date_time);
        
        //再看ERROR資料夾
        $begin_date_dir = PARSING_FILE_PROCESS_ERROR_PATH . $begin_date_str . "\\";
        //echo "先看ERROR資料夾 >> $begin_date_dir".PHP_EOL;
        $files = $this->chkMatchedFiles($files, $begin_date_dir, $begin_date_time, $end_date_time);
        
        sort($files);   //由小到大排序
        //print_r($files);
        return $files;
    }
    
    /**
     **取得HeNBGW目錄下所有檔案
     * @return array
     */
    public function getLocalFile() {
        $files = array();
        $tmp_files = array();
        
        foreach (new RecursiveIteratorIterator(new RecursiveDirectoryIterator(PARSING_FILE_lOCAL_PATH)) as $filepath)
        {
            // filter out "." and ".."
            if ($filepath->isDir()) continue;
           
            $mtime = $filepath->getMTime();
            $filename = $filepath->getFilename();
            $old_path = str_replace("\\", "/", $filepath);
            
            // 將檔案剪下到 Parsing system 處理資料夾
            $new_path = PARSING_FILE_PROCESS_PATH . $filename;
            $file_moved = rename($old_path, $new_path);
            
            if ($file_moved) {
                $tmp_files[$mtime][] = $new_path;
            }
        }
        
        ksort($tmp_files);
        
        foreach ($tmp_files as $path_array) {
            foreach ($path_array as $path) {
                array_push($files, $path);
            }
        }
        
        return $files;
    }
    
    /**
     **取得cDR目錄下所有檔案
     * @return array
     */
    public function getLocalCDRFile() {
        $files = array();
        $tmp_files = array();
        
        foreach (new RecursiveIteratorIterator(new RecursiveDirectoryIterator(CDR_DECODE_FILE_LOCAL_PATH)) as $filepath)
        {
            // filter out "." and ".."
            if ($filepath->isDir()) continue;
            
            $mtime = $filepath->getMTime();
            $filename = $filepath->getFilename();
            $old_path = str_replace("\\", "/", $filepath);
            
            // 將檔案剪下到 CDR Parsing 處理資料夾
            $new_path = CDR_DECODE_FILE_PROCESS_PATH . $filename;
            $file_moved = rename($old_path, $new_path);
            
            if ($file_moved) {
                $tmp_files[$mtime][] = $new_path;
                //array_push($files, $new_path);
            }
        }
        
        ksort($tmp_files);
        
        foreach ($tmp_files as $path_array) {
            foreach ($path_array as $path) {
                array_push($files, $path);
            }
        }
        
        return $files;
    }
}

class FilesystemDateFilter extends RecursiveFilterIterator
{
    protected $earliest_date;
    protected $lastest_date;
    
    public function __construct(RecursiveIterator $it, $earliest_date, $lastest_date)
    {
        $this->earliest_date = $earliest_date;
        $this->lastest_date = $lastest_date;
        parent::__construct($it);
    }
    
    public function accept()
    {
        /*
        echo "fileName: ".$this->getFilename().", MTime: ".$this->getMTime().", earliest_date: ".$this->earliest_date.", lastest_date: ".$this->lastest_date.PHP_EOL;
        echo ">>> ifFile: ".$this->isFile().", MTime >= earliest_date: ".($this->getMTime() >= $this->earliest_date).", MTime < lastest_date: ".($this->getMTime() < $this->lastest_date).PHP_EOL;
        echo ">>>>>> return: ".(! $this->isFile() || ( $this->getMTime() >= $this->earliest_date && $this->getMTime() < $this->lastest_date )).PHP_EOL;
        */
        return (! $this->isFile() || ( $this->getMTime() >= $this->earliest_date && $this->getMTime() < $this->lastest_date ));
    }
    
    public function getChildren()
    {
        return new static ($this->getInnerIterator()->getChildren(), $this->earliest_date, $this->lastest_date);
    }
} 
