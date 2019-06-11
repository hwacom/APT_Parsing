<?php
namespace Hwacom\APT_Parsing\utils;

class GuidUtils {
    
    public function getGuid() {
        mt_srand((double)microtime()*10000);//optional for php 4.2.0 and up.
        $charid = strtoupper(md5(uniqid(rand(), true)));
        $uuid = substr($charid, 0, 8)
               .substr($charid, 8, 4)
               .substr($charid,12, 4)
               .substr($charid,16, 4)
               .substr($charid,20,12);
        return $uuid;
        
        //If it's hex without dashes, it'll be varchar(32)
        //Output ex: 90947CC8C5750A7E239142B80343E399
    }
}