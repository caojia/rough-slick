package io.scalac.slick.db

import io.scalac.slick.db.Dao.SaleRecord

import scala.slick.driver.MySQLDriver.simple._
import scala.slick.jdbc.GetResult
import scala.slick.jdbc.StaticQuery.interpolation

object PlainDao extends Dao with DbProvider {

  def fetchSales(minTotal: BigDecimal): Seq[SaleRecord] = db.withSession { implicit session =>
    sql"""SELECT SQL_NO_CACHE purchaser.name, supplier.name, product.name, sale.total
          FROM sale join purchaser join product join supplier
          ON (sale.purchaser_id = purchaser.id AND
              sale.product_id = product.id AND
              product.supplier_id = supplier.id)
          WHERE sale.total >= $minTotal"""
    .as[(String, String, String, BigDecimal)].list.map(SaleRecord.tupled)
  }
}
