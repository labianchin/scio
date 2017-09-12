/*
 * Copyright 2017 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.extra.transforms

import com.spotify.scio.extra.transforms.DoFnWithResource.ResourceType
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.transforms.windowing.{BoundedWindow, PaneInfo}
import org.apache.beam.sdk.values.{PCollectionView, TupleTag}
import org.joda.time.Instant
import org.scalacheck._
import org.scalacheck.commands.Commands

import scala.collection.mutable.{Buffer => MBuffer}
import scala.concurrent.{Future, Promise}
import scala.util.Random

object AsyncDoFnSpec extends Properties("AsyncDoFn") {
//  property("ScalaDoFn") = AsyncDoFnCommands.property()
  property("ScalaDoFn") = {
    val fn = new AsyncDoFnTester
    fn.request()
//    fn.request()
//    fn.complete()
    fn.complete()
    fn.finishBundle()
    true
  }
}

object AsyncDoFnCommands extends Commands {

  case class AsyncDoFnState(total: Int, pending: Int, complete: Int)

  override type State = AsyncDoFnState
  override type Sut = AsyncDoFnTester

  override def canCreateNewSut(newState: AsyncDoFnState,
                               initSuts: Traversable[AsyncDoFnState],
                               runningSuts: Traversable[AsyncDoFnTester]): Boolean = true

  override def newSut(state: AsyncDoFnState): AsyncDoFnTester = new AsyncDoFnTester

  override def destroySut(sut: AsyncDoFnTester): Unit = {}

  override def initialPreCondition(state: AsyncDoFnState): Boolean = state ==
    AsyncDoFnState(0, 0, 0)

  override def genInitialState: Gen[AsyncDoFnState] = Gen.const(AsyncDoFnState(0, 0, 0))

  override def genCommand(state: AsyncDoFnState): Gen[Command] =
    Gen.frequency(
      (75, Gen.const(Request)),
      (20, Gen.const(Complete)),
      (1, Gen.const(FinishBundle)))

  case object Request extends UnitCommand {
    override def preCondition(state: AsyncDoFnState): Boolean = true
    override def postCondition(state: AsyncDoFnState, success: Boolean): Prop = success

    override def run(sut: AsyncDoFnTester): Unit = sut.request()
    override def nextState(state: AsyncDoFnState): AsyncDoFnState =
      state.copy(total = state.total + 1, pending = state.pending + 1)
  }

  case object Complete extends UnitCommand {
    override def preCondition(state: AsyncDoFnState): Boolean = state.pending > 0
    override def postCondition(state: AsyncDoFnState, success: Boolean): Prop = success

    override def run(sut: AsyncDoFnTester): Unit = sut.complete()
    override def nextState(state: AsyncDoFnState): AsyncDoFnState =
      state.copy(pending = state.pending - 1, complete = state.complete + 1)
  }

  case object FinishBundle extends UnitCommand {
    override def preCondition(state: AsyncDoFnState): Boolean = true
    override def postCondition(state: AsyncDoFnState, success: Boolean): Prop = success

    override def run(sut: AsyncDoFnTester): Unit = sut.finishBundle()
    override def nextState(state: AsyncDoFnState): AsyncDoFnState =
      state.copy(pending = 0, complete = state.complete + 1)
  }

}

class AsyncDoFnTester {
  private var nextElement = 0
  private val pending = MBuffer.empty[(Int, Promise[String])]
  private val outputBuffer = MBuffer.empty[String]

  private val fn = {
    val f = new ScalaAsyncDoFn[Int, String, Unit] {
      override def getResourceType: ResourceType = ResourceType.PER_CLASS
      override def processElement(input: Int): Future[String] = {
        val p = Promise[String]()
        pending.append((input, p))
        p.future
      }
      override def createResource(): Unit = Unit
    }
    f.setup()
    f.startBundle()
    f
  }

  private val processContext = new fn.ProcessContext {
    override def getPipelineOptions = ???
    override def sideInput[T](view: PCollectionView[T]): T = ???
    override def pane(): PaneInfo = ???
    override def updateWatermark(watermark: Instant): Unit = ???
    override def output[T](tag: TupleTag[T], output: T): Unit = ???
    override def outputWithTimestamp(output: String, timestamp: Instant): Unit = ???
    override def outputWithTimestamp[T](tag: TupleTag[T], output: T, timestamp: Instant): Unit = ???

    override def timestamp(): Instant = new Instant(nextElement)
    override def element(): Int = nextElement
    override def output(output: String): Unit = outputBuffer.append(output)
  }

  private val finishBundleContext = new fn.FinishBundleContext {
    override def getPipelineOptions: PipelineOptions = ???
    override def output[T](tag: TupleTag[T],
                           output: T,
                           timestamp: Instant,
                           window: BoundedWindow): Unit = ???
    override def output(output: String, timestamp: Instant, window: BoundedWindow): Unit =
      outputBuffer.append(output)
  }

  def request(): Unit = {
    fn.processElement(processContext, null)
    nextElement += 1
  }

  def complete(): Unit = {
    val i = Random.nextInt(pending.size)
    val (input, promise) = pending(i)
    promise.success(input.toString)
    pending.remove(i)
  }

  def finishBundle(): Unit = {
    if (nextElement != pending.size + outputBuffer.size) {
      println("!!!!!", nextElement, pending.map(_._1), outputBuffer)
      assert(nextElement == pending.size + outputBuffer.size)
    }
    nextElement = 0
    pending.foreach { case (input, promise) =>
      promise.success(input.toString)
    }
    pending.clear()
    fn.finishBundle(finishBundleContext)
  }

}
