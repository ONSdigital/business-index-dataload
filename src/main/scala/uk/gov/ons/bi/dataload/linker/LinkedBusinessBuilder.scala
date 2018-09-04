package uk.gov.ons.bi.dataload.linker

import org.apache.spark.rdd.RDD

import uk.gov.ons.bi.dataload.model._
import uk.gov.ons.bi.dataload.utils.Transformers


object LinkedBusinessBuilder {
  // NOTE:
  // This needs to be an object, not a Singleton, because we get weird Spark "Task not serializable"
  // errors when there is a lot of nested RDD processing around here. Might be better in Spark 2.x?


  // This object contains Spark-specific code for processing RDDs and DataFrames.
  // Non-Spark transformations are in the separate Transformers object.

  def convertUwdsToBusinessRecords(uwds: RDD[UbrnWithData]): RDD[Business] = {
    // Now we can group data for same UBRN back together
    val grouped: RDD[(BiTypes.Ubrn, Iterable[UbrnWithData])] = uwds.map { r => (r.ubrn, r) }.groupByKey()
    val uwls: RDD[UbrnWithList] = grouped.map { case (ubrn, uwds) => UbrnWithList(ubrn, uwds.toList) }
    // Convert each UBRN group to a Business record
    uwls.map(Transformers.buildBusinessRecord)
  }

  // ***************** Link UBRN to Company/VAT/PAYE data **************************

  def getLinkedCompanyData(uwks: RDD[UbrnWithKey], chs: RDD[(String, CompanyRec)]): RDD[UbrnWithData] = {

    // Join Links to corresponding data

    val linkedData: RDD[UbrnWithData] = uwks.filter { r => r.src == CH }.map { r => (r.key, r) }
      .join(chs)
      .map { case (key, (uwk, data))
      => UbrnWithData(uwk.ubrn, uwk.src, data)
      }

    linkedData
  }

  def getLinkedVatData(uwks: RDD[UbrnWithKey], vats: RDD[(String, VatRec)]): RDD[UbrnWithData] = {

    // Join Links to corresponding data

    val linkedData: RDD[UbrnWithData] = uwks.filter { r => r.src == VAT }.map { r => (r.key, r) }
      .join(vats)
      .map { case (key, (uwk, data))
      => UbrnWithData(uwk.ubrn, uwk.src, data)
      }

    linkedData
  }

  def getLinkedPayeData(uwks: RDD[UbrnWithKey], payes: RDD[(String, PayeRec)]): RDD[UbrnWithData] = {

    // Join Links to corresponding data

    val linkedData: RDD[UbrnWithData] = uwks.filter { r => r.src == PAYE }.map { r => (r.key, r) }
      .join(payes)
      .map { case (key, (uwk, data))
      => UbrnWithData(uwk.ubrn, uwk.src, data)
      }

    linkedData
  }

  def getLinksAsUwks(links: RDD[LinkRec]): RDD[UbrnWithKey] = {
    // explodeLink() converts each nested Link record to a sequence of (UBRN, type, key) triples.
    // flatMap(identity) then turns it from an RDD[Seq[UbrnWithKey]] into an
    // RDD[UbrnWithKey], which is what we want.

    val uwks: RDD[UbrnWithKey] = links.map { ln => Transformers.explodeLink(ln) }.flatMap(identity)
    uwks
  }

}
