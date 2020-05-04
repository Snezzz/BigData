package second

import java.math.BigInteger

val list = arrayListOf<List<Int>>() //список разложений на множители для каждого числа

fun main(args:Array<String>){
    val startTime = System.currentTimeMillis()
    val result = numberOfPrimeFactors()
    val finishTime = System.currentTimeMillis()
    println("Затрачено времени(в мс):${finishTime-startTime}")
    println("Количество чисел:${list.size}")
    println("Ответ:$result")
}

/**
 * Подсчитывает, какое суммарное количество простых множителей
 * присутствует при факторизации всех чисел
 */
fun numberOfPrimeFactors():Int{
    val listOfNumbers = getData("data.txt") //получаем список данных
    //для каждого числа последовательно считаем факторизацию
    listOfNumbers.forEach { it ->
        val fact = arrayListOf<Int>()
        factorization(it, fact)
    }
    return getCount()
}

fun getCount():Int{
    var count = 0
    for(el in list){
        count+=el.size
    }
    return count
}

/**
 * Рекурсивная функция по разложению числа на простые множители
 *
 * @param currentNumber - число, которое необходимо разложить
 * @param fact - список всех простых множителей, на которые мы раскладываем число
 */
fun factorization(currentNumber: BigInteger,fact:MutableList<Int>) {
    var flag = false
    var number = 2
    while (!flag) {
        //текущее число является простым
        if (isPrime(number)) {
            //делится нацело
            if (currentNumber % number.toBigInteger() == 0.toBigInteger()) {
                flag = true
                val result = currentNumber / number.toBigInteger()
                fact.add(number) //добавили множитель
                if(result != 1.toBigInteger())
                    factorization(result, fact)
                else
                    list.add(fact)
            }
        }
        number+=1
    }
}

/**
 * Проверяем, является ли текущее число простым
 * @param number - число, что мы рассматриваем
 */
private fun isPrime(number: Int): Boolean {
    if (number <= 1)
        return false
    for (i in 2 until number)
        if (number % i == 0)
            return false
    return true
}

