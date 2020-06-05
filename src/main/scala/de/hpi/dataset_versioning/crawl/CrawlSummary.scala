package de.hpi.dataset_versioning.crawl

import de.hpi.dataset_versioning.util.CrawlSummarizer

object CrawlSummary extends App {

  val summarizer = new CrawlSummarizer(args(0))
  summarizer.allTimeChangeSummary()
}
