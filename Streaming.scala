package Partidor


import org.apache.spark.util.LongAccumulator
import org.apache.commons.lang3.StringUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, dstream}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ListBuffer
import scala.io.Source

object Partidor {

 val checkpointDir = "./checkpoints"


  def main(args: Array[String]): Unit = {


   def crearStreamingContext(): StreamingContext = {
  println("Estoy creando el entorno")

    // Cargo las palabras vacias
    val STOP_WORDS=new ListBuffer[String]()                           // Creo la lista
    val fichero=Source.fromFile("src/main/resources/stopwords.txt")   // Abro el fichero
    for (line <- fichero.getLines) STOP_WORDS += line                 // Leo las palabras
    fichero.close()                                                   // Cierro conexión con el fichero

    val sesion=SparkSession                                           // Abrimos sesión con Spark
      .builder
      .appName("Partidor")                                      // Le pongo un nombre a la aplicación para verla dsde el UI
      .master("local[2]")                                      // Abrir un Spark en local con 2 CPUs
                                                                      // Nota: Esto va fuera en una applicación real que se pueda ejecutar
                                                                      // en cualquier cluster Spark
      .getOrCreate()                                                  // Recupero la sesión

    val streaming = new StreamingContext(sesion.sparkContext, Seconds(5))
     streaming.checkpoint(checkpointDir)

    val textos: DStream[String]=streaming.socketTextStream("localhost", 10000)
    val totalPalabrasEliminadas = sesion.sparkContext.longAccumulator("totalpalabrasEliminadas")
    val STOP_WORDS_ENVIADO = sesion.sparkContext.broadcast(STOP_WORDS)


    import sesion.implicits._         // Activo funciones implicitas en la sesión para poder transformar a Dataframe los RDDs

    val textosProcesados:dstream.DStream[String]=textos
      
    /* quitar acentos */                                        .map(StringUtils.stripAccents)
    /* a minúsculas */                                          .map(_.toLowerCase())
    /* separar palabras */                                      .flatMap(_.split("[^#\\w]+"))
    /* quitar palabras vacias */                                .filter(_.length>0)
    /* quitar stop words */                                     .filter(filtrarPalabras(_,STOP_WORDS_ENVIADO.value, totalPalabrasEliminadas))
     

    textosProcesados.foreachRDD(rdd => {
      rdd

        /* a dataframe */                                       .toDF()
        /* agrupar por palabra */                               .groupBy("value")
        /* contarlas */                                         .count()
        /* ordenarlas */                                        .sort(desc("count"))
        /* solo me quedo con 10 */                              .limit(10)
        /* las muestro */                                       .show()
       
        println("He borrado: " + totalPalabrasEliminadas.value + " palabras")
      }
    )
    streaming
   }

    var streamingSession: StreamingContext = StreamingContext.getActiveOrCreate(checkpointDir, crearStreamingContext)

    streamingSession.start()
    streamingSession.awaitTermination()
  }

  def filtrarPalabras(palabra:String, STOP_WORDS:ListBuffer[String], totalPalabrasEliminadas:LongAccumulator): Boolean = {
    val seQueda = !STOP_WORDS.contains(palabra)
    if (!seQueda) totalPalabrasEliminadas.add(1)
    seQueda
  }

}
