package br.ufmg.cs.lib.privacy.kanonymity.examples

import br.ufmg.cs.lib.privacy.kanonymity.Mondrian
import br.ufmg.cs.util.Timeable

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SiapeApp extends Timeable {
  def main(args: Array[String]) {
    // args
    val siapePath = args(0)
    val k = args(1).toInt
    val mode = args(2)

     
   val keyColumns = List("faixa_etaria", "habilitacao_profissional",
      "uf_residencial", "funcao")
    val sensitiveColumns = List("remuneracao")

    val spark = SparkSession.builder().
    master("local[4]").
      config("spark.sql.shuffle.partitions", "8").
      getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val rawData = readSiape(spark, siapePath)

    val mondrian = new Mondrian(rawData, keyColumns, sensitiveColumns, k, mode)
    val mondrianRes = mondrian.result
    val resultDataset = mondrianRes.resultDataset.cache
    println (s"result = ${mondrianRes}")
    println (s"number of anonymized records = ${resultDataset.count}")
    resultDataset.show
    println (s"ncp = ${mondrianRes.ncp}")

    val resultDatasetRev = mondrianRes.resultDatasetRev 
    resultDatasetRev.show

    val anonymizedData = mondrianRes.anonymizedData
    anonymizedData.show
    
    spark.stop()
  }


  // mes,orgao_superior,orgao_agrupador,orgao,id_servidor,vinculo_servidor,
  // cpf_servidor,titulo_eleitor,nome_servidor,idade,faixa_etaria,
  // sexo,origem_etnica,estado_civil,ano_prev_aposentadoria,
  // habilitacao_profissional,grupo_escolaridade,uf_residencial,
  // funcao,nivel_funcao,ano_ingresso_serv_publico,mes_ingresso_serv_publico,
  // ingresso_serv_publico,situacao_funcional,situacao_vinculo,
  // grupo_situacao_vinculo,regime_juridico,jornada_trabalho,
  // jornada_cargo,cargo,data_ingresso_cargo,grupo_cargo_basico,nivel_cargo,
  // area_atuacao_cargo,atividade_governo,natureza_juridica,
  // grupo_natureza_juridica,upag,nome_und_organizacao,
  // remuneracao,rendimento,desconto
  
  case class SiapeRecord(mes: String, orgao_superior: String,
    orgao_agrupador: String,
    orgao: String, id_servidor: String, vinculo_servidor: String,
    cpf_servidor: String, titulo_eleitor: String, nome_servidor: String,
    idade: String, faixa_etaria: String, sexo: String, origem_etnica: String,
    estado_civil: String, ano_prev_aposentadoria: String,
    habilitacao_profissional: String, grupo_escolaridade: String,
    uf_residencial: String, funcao: String, nivel_funcao: String,
    ano_ingresso_serv_publico: String, mes_ingresso_serv_publico: String,
    ingresso_serv_publico: String, situacao_funcional: String,
    situacao_vinculo: String, grupo_situacao_vinculo: String,
    regime_juridico: String, jornada_trabalho: String, jornada_cargo: String,
    cargo: String, data_ingresso_cargo: String, grupo_cargo_basico: String,
    nivel_cargo: String, area_atuacao_cargo: String, atividade_governo: String,
    natureza_juridica: String, grupo_natureza_juridica: String, upag: String,
    nome_und_organizacao: String, remuneracao: String, rendimento: String,
    desconto: String)

  def readSiape(spark: SparkSession, path: String): Dataset[Row] = {
    import spark.implicits._
    val siape = spark.sparkContext.textFile(path).
      map(_ split ",").flatMap { fields =>
        try {
          Iterator(SiapeRecord(
            fields(0).replaceAll("\"", "").trim,
            fields(1).replaceAll("\"", "").trim,
            fields(2).replaceAll("\"", "").trim,
            fields(3).replaceAll("\"", "").trim,
            fields(4).replaceAll("\"", "").trim,
            fields(5).replaceAll("\"", "").trim,
            fields(6).replaceAll("\"", "").trim,
            fields(7).replaceAll("\"", "").trim,
            fields(8).replaceAll("\"", "").trim,
            fields(9).replaceAll("\"", "").trim,
            fields(10).replaceAll("\"", "").trim,
            fields(11).replaceAll("\"", "").trim,
            fields(12).replaceAll("\"", "").trim,
            fields(13).replaceAll("\"", "").trim,
            fields(14).replaceAll("\"", "").trim,
            fields(15).replaceAll("\"", "").trim,
            fields(16).replaceAll("\"", "").trim,
            fields(17).replaceAll("\"", "").trim,
            fields(18).replaceAll("\"", "").trim,
            fields(19).replaceAll("\"", "").trim,
            fields(20).replaceAll("\"", "").trim,
            fields(21).replaceAll("\"", "").trim,
            fields(22).replaceAll("\"", "").trim,
            fields(23).replaceAll("\"", "").trim,
            fields(24).replaceAll("\"", "").trim,
            fields(25).replaceAll("\"", "").trim,
            fields(26).replaceAll("\"", "").trim,
            fields(27).replaceAll("\"", "").trim,
            fields(28).replaceAll("\"", "").trim,
            fields(29).replaceAll("\"", "").trim,
            fields(30).replaceAll("\"", "").trim,
            fields(31).replaceAll("\"", "").trim,
            fields(32).replaceAll("\"", "").trim,
            fields(33).replaceAll("\"", "").trim,
            fields(34).replaceAll("\"", "").trim,
            fields(35).replaceAll("\"", "").trim,
            fields(36).replaceAll("\"", "").trim,
            fields(37).replaceAll("\"", "").trim,
            fields(38).replaceAll("\"", "").trim,
            fields(39).replaceAll("\"", "").trim,
            fields(40).replaceAll("\"", "").trim,
            fields(41).replaceAll("\"", "").trim
          ))
        } catch {
          case e: NumberFormatException => Iterator.empty
          case e: Throwable => throw e
        }
      }.mapPartitionsWithIndex { case (idx, iter) =>
        if (idx == 0) {
          iter.drop(1)
        } else {
          iter
        }
      }.toDF()
    siape
  }
}
