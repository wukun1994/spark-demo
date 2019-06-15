package com.atguigu.sparkmall.offline

package object app {

    def isNotEmpty(text:String):Boolean=text!=null && text.length>0
    def isEmpty(text:String):Boolean = !isEmpty(text)
}
