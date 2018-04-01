package com.z.utils

import java.security.MessageDigest

/**
  * Created by root on 18-4-1.
  */
object Md5 {

  def md5String(s: String) = {
    MessageDigest.getInstance("MD5").digest(s.getBytes).map("%02x".format(_)).mkString

  }

  def md5Int(s: String) = {
    BigInt(md5String(s), 16)
  }

}
