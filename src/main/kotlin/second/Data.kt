package second

import java.io.File
import java.io.InputStream
import java.math.BigInteger



fun main(args: Array<String>){
    dataGeneration(2000)
}

/**
 * Генерирует 128-битные случайные целые числа
 * и записывает их в файл
 * @param amount - количество случайных чисел
 */
fun dataGeneration(amount: Int){
    val data = arrayListOf<Int>()
    for (i in (0..amount)){
        data.add((Math.random() * 0x10000).toInt())
    }
    File("data.txt").printWriter().use { out ->
        data.forEach {
            out.println("$it")
        }
    }
}

/**
 * Читаем числа из файла
 * @param file - файл для чтения
 */
fun getData(file: String):List<BigInteger>{
    val inputStream: InputStream = File(file).inputStream()
    val lineList = mutableListOf<BigInteger>()
    inputStream.bufferedReader().useLines { lines -> lines.forEach { lineList.add(it.toBigInteger())} }
    return lineList
}
