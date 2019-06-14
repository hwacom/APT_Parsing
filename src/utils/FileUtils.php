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
     **�P�_��Ƨ����|�O�_�s�b�A���s�b�h�إ�
     * @param $dir_path
     */
    public function checkIfDirExistsOrCreate($dir_path) {
        if (!file_exists($dir_path) && !is_dir($dir_path)) {
            mkdir($dir_path, 0777, true);
        }
    }
    
    /**
     ** ���X�ŦX����d�򤺪��ɮ׸��|�A���Array���^��
     * @param $files_array
     * @param $dir_path
     * @param $begin_date_time
     * @param $end_date_time
     * @return
     */
    public function chkMatchedFiles($files_array, $dir_path, $begin_date_time, $end_date_time) {
        //�P�_��Ƨ��O�_�s�b OR �Ы�
        $this->checkIfDirExistsOrCreate($dir_path);
        
        $directory = new RecursiveDirectoryIterator($dir_path);
        $filter = new FilesystemDateFilter($directory, $begin_date_time->getTimestamp(), $end_date_time->getTimestamp());
        
        foreach(new RecursiveIteratorIterator($filter) as $path) {
            array_push($files_array, $path->getPathname());
        }
        
        return $files_array;
    }
    
    /**
     **���oHeNBGW�ƥ��ؿ��U�A�ŦX����϶������Ҧ��ɮ�
     * @param $begin_time
     * @param $end_time
     */
    public function getLocalFileBySpecifyInterval($begin_date_time, $end_date_time) {
        $files = array();
        
        $begin_date_str = $begin_date_time->format("Y-m-d");
        $begin_time_str = $begin_date_time->format("H:i");
        
        if ($begin_time_str === "00:00") {
            //�p�G��J���_�l�ɶ����u00:00�v�A�h�����e���@�Ѩ��o�e�@�ѳ̫�@���ɮסu23:55�v
            $pre_date_time = date_create_from_format("Y-m-d H:i", $begin_date_str." 23:55");
            $pre_end_date_time = date_create_from_format("Y-m-d H:i", $begin_date_str." 00:00");
            date_sub($pre_date_time, date_interval_create_from_date_string('1 days'));
            
            $pre_date_str = $pre_date_time->format("Y-m-d");
            
            //����SUCCESS��Ƨ�
            $pre_date_dir = PARSING_FILE_PROCESS_SUCCESS_PATH . $pre_date_str . "\\";
            //echo "����SUCCESS��Ƨ� >> $pre_date_dir".PHP_EOL;
            $files = $this->chkMatchedFiles($files, $pre_date_dir, $pre_date_time, $pre_end_date_time);
            
            //�A��ERROR��Ƨ�
            $pre_date_dir = PARSING_FILE_PROCESS_ERROR_PATH . $pre_date_str . "\\";
            //echo "����ERROR��Ƨ� >> $pre_date_dir".PHP_EOL;
            $files = $this->chkMatchedFiles($files, $pre_date_dir, $pre_date_time, $pre_end_date_time);
            
            $begin_date_time = date_create_from_format("Y-m-d H:i", $begin_date_str." 00:00");
            
        } else {
            //Begin time �����e��5�����A��X�e�@�����ƾڤ~�వ�p��
            date_sub($begin_date_time, date_interval_create_from_date_string('5 minutes'));
        }
        
        //����SUCCESS��Ƨ�
        $begin_date_dir = PARSING_FILE_PROCESS_SUCCESS_PATH . $begin_date_str . "\\";
        //echo "����SUCCESS��Ƨ� >> $begin_date_dir".PHP_EOL;
        $files = $this->chkMatchedFiles($files, $begin_date_dir, $begin_date_time, $end_date_time);
        
        //�A��ERROR��Ƨ�
        $begin_date_dir = PARSING_FILE_PROCESS_ERROR_PATH . $begin_date_str . "\\";
        //echo "����ERROR��Ƨ� >> $begin_date_dir".PHP_EOL;
        $files = $this->chkMatchedFiles($files, $begin_date_dir, $begin_date_time, $end_date_time);
        
        sort($files);   //�Ѥp��j�Ƨ�
        //print_r($files);
        return $files;
    }
    
    /**
     **���oHeNBGW�ؿ��U�Ҧ��ɮ�
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
            
            // �N�ɮװŤU�� Parsing system �B�z��Ƨ�
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
     **���ocDR�ؿ��U�Ҧ��ɮ�
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
            
            // �N�ɮװŤU�� CDR Parsing �B�z��Ƨ�
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
