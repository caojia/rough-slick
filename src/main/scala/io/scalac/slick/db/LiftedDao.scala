package io.scalac.slick.db

import io.scalac.slick.db.Dao.SaleRecord

import scala.slick.driver.MySQLDriver.simple._

object LiftedDao extends Dao with DbProvider {

  val suppliers = TableQuery[Supplier]
  val purchasers = TableQuery[Purchaser]
  val products = TableQuery[Product]
  val sales = TableQuery[Sale]

  val salesQuery = {
    // Join the four tables
    val salesJoin = sales join purchasers join products join suppliers on {
      case (((sale, purchaser), product), supplier) =>
        sale.productId === product.id &&
        sale.purchaserId === purchaser.id &&
        product.supplierId === supplier.id
    }

    // Add a predicate and extract relevant columns
    def query(minTotal: Column[BigDecimal]) = for {
      (((sale, purchaser), product), supplier) <- salesJoin
      if sale.total >= minTotal
    } yield (purchaser.name, supplier.name, product.name, sale.total)

    // Precompile the query so it's efficiently reusable
    //Compiled(query _)
    query _
  }

  def fetchSalesWithoutSlickJoin(minTotal: BigDecimal): List[SaleRecord] = db.withSession { implicit session =>
    val query = for (sale <- sales;
         purchaser <- purchasers;
         product <- products;
         supplier <- suppliers;
         if (sale.productId === product.id &&
          sale.purchaserId === purchaser.id &&
          product.supplierId === supplier.id))
      yield (purchaser.name, supplier.name, product.name, sale.total)

    query.filter(_._4 >= minTotal).list.map(SaleRecord.tupled)
  }


  def fetchSales(minTotal: BigDecimal): List[SaleRecord] = db.withSession { implicit session =>
    salesQuery(minTotal).list.map(SaleRecord.tupled)
  }

  def countSales(): Int = db.withSession { implicit session =>
    sales.length.run
  }

  def fetchProductNamesByIds(ids: List[Int]): List[String] = db.withSession { implicit session =>
    val query = products.filter(_.id inSet ids).map(_.name)
    query.list
  }

  def insertSupplier(id: Int, name: String) = db.withSession { implicit session =>
    suppliers.map(s => (s.id, s.name)) += (id, name)
  }

  def insertProduct(id: Int, supplierId: Int, name: String) = db.withSession { implicit session =>
    products += (id, supplierId, name)
  }

  def insertPurchaser(id: Int, name: String) = db.withSession { implicit session =>
    purchasers.map(p => (p.id, p.name)) += (id, name)
  }

  def insertSale(id: Int, purchaserId: Int, productId: Int, total: BigDecimal) = db.withSession { implicit session =>
    sales += (id, purchaserId, productId, total)
  }

  def deleteAll() = db.withSession { implicit session =>
    sales.delete
    products.delete
    purchasers.delete
    suppliers.delete
  }
}

object LiftedWithoutJoinDao extends Dao with DbProvider {

  def fetchSales(minTotal: BigDecimal): Seq[SaleRecord] = LiftedDao.fetchSalesWithoutSlickJoin(minTotal)
}
