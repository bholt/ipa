package owl

import scala.concurrent.Future

object Common {

}

trait TableGenerator {
  def create(): Future[Unit]
  def truncate(): Future[Unit]
}