package org.broadinstitute.hail.driver

object FilterExportTrios extends SuperCommand {
  def name = "filterexporttrios"

  def description = "Filter and export trio genotypes"

  register(FilterExportTriosSamocha)
  register(FilterExportTriosExpr)
}
