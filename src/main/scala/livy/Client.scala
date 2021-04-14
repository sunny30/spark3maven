package livy

import com.google.gson.Gson
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.SparkSession

object Client {

  val sparkSession = SparkSession.builder
    .master("local[1]")
    .appName("app_1")
    .getOrCreate()


  def squareMe(n: Int): Double = {
    n*n
  }


/*
curl -X POST --data '{"kind": "spark"}' -H "Content-Type: application/json" localhost:8998/sessions
 */

  def main(args: Array[String]): Unit = {
    val sessionId: Int =0


      // From here we need to do a POST request with the number we want to square

      val numbToSquare: Int = 2

      val post = new HttpPost(s"http://localhost:8998/session/$sessionId/statements")

      post.setHeader("Content-type", "application/json")

      val body: String = s"""{'code': 'squareMe($numbToSquare)'}""" // JSON STRING of the code we want to run
    val jsonBody = new Gson().toJson(body)
    post.setEntity(new StringEntity(jsonBody))
    post.setEntity(new StringEntity(body)) // Add our code to the body


    val response = (new DefaultHttpClient).execute(post) // Execute our request

    // Finally, print out the results

    val responseBody = EntityUtils.toString(response.getEntity)



    println(response.toString)
  }

}
