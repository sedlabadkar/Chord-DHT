import akka.actor._
import akka.actor.ActorRef
import akka.util.Timeout
import java.lang.Object
import java.lang.String
import java.security.MessageDigest
import scala.util.control.Breaks._
import scala.collection.mutable.MutableList
import akka.routing.RoundRobinPool
import scala.concurrent._
import scala.concurrent.duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.Random


case class init (id:Int, numReq:Int, numNodes:Int, allActors:List[ActorRef])
case class start (numNodes:Int, numRequests:Int)
case class initiateLookup(key:String, requestingNode:ActorRef, lookupType:String, hopCount:Int)
case class notifyRequestingActor(requestedKey:String, responsibleNodeID:Int, lookupType:String, hopCount:Int)
case class initFingerTable(id:Int)
case class neighborList(fT:Array[(Int, ActorRef,String,String)])
case class updateFingerTable(id:Int)
case class join (ref:ActorRef)
case class joined (id:Int)

class node extends Actor {
  var m = 0
  var mySuccessor/*ActorRef*/:(Int, ActorRef, String, String) = null //To keep track of this node's successor 
  var myPredecessor:ActorRef = null //To keep track of this node's predecessor
  var selfID:Int = -1 
  var successor:Int = -1
  var fingerTable:Array[(Int,ActorRef,String,String)] = new Array[(Int,ActorRef,String,String)](128) //This node's finger table. Index 0 should be the successor 
  var totalHopCount:Int = 1
  var requestReturnCount:Int = 0
  var fingerNodeReturnCount:Int = 0
  var newFingerTable:Array[(Int,ActorRef,String,String)] = new Array[(Int,ActorRef,String,String)](128) //(i, ActorRef, actorHash, intervalStartHash)
  var joiningNodeID:Int = -1
  var joiningNodeRef:ActorRef = null
  var numRequests:Int = 0
  var cancellable:Cancellable = null
  val dur = Duration.create(10, scala.concurrent.duration.MILLISECONDS);
  val delay = Duration.create(1000, scala.concurrent.duration.MILLISECONDS);
  var bootstrap:ActorRef = null
  val r = new Random(31)
  var reqCount:Int = 0
  var actorList:List[ActorRef] = Nil
  
  def receive = {
    case "start" => {
      //println ("start" + selfID)
      cancellable = context.system.scheduler.schedule(selfID * delay, dur, self, "startLookup")
    }   
    case "startLookup" => {
     var i = 0
     reqCount = reqCount + 1
     if (reqCount == numRequests)
       cancellable.cancel
     find_successor(getHashKeyForString(r.nextString(10)), self, "lookup", 1)
    }
    case join(ref:ActorRef) => {
      //println ("JOin " + selfID)
      join (ref)
    }
    case init(id:Int, numReq:Int, numNodes:Int, allactors:List[ActorRef]) => {
      m = numNodes
      numRequests = numReq
      selfID = id
      bootstrap = sender
      actorList = allactors
      join (null)
    }
    case initiateLookup(key:String, requestingNode:ActorRef, lookupType:String, hopCount:Int) => {
      //println ("initiateLookup " + selfID + " key = " + key)
      var realRequestingNode:ActorRef = null
      if (requestingNode == null)
        realRequestingNode = self
      else 
        realRequestingNode = requestingNode
      
      find_successor(key, realRequestingNode, lookupType, hopCount)
    }  
    case notifyRequestingActor(requestedKey:String, responsibleNodeID:Int, lookupType:String, hopCount:Int) => {
      var responsibleNodeRef:ActorRef = null
      var responsibleNodeIDHash:String = getHashKeyForString(responsibleNodeID.toString())
      //println (selfID + " Responsible Node found =" + responsibleNodeID + " Hash = " + responsibleNodeIDHash)
      //println (selfID + " : String Located at :" + responsibleNodeID + " hopCount " + hopCount)
      responsibleNodeRef = sender
      
      if (lookupType.equals("newnode")) //This was a lookup for join
      { 
        fingerNodeReturnCount = fingerNodeReturnCount + 1
        var i = 0
        //update the newfingertable with this tuple
        breakable{
          while (i < m)
          {
            var (id:Int, ref:ActorRef, actorHashString:String, intervalStart:String) = newFingerTable(i)
            if (intervalStart.equals(requestedKey)) //This is the tuple that needs updating
            {
              ref = responsibleNodeRef
              actorHashString = responsibleNodeIDHash
              newFingerTable(i) = (id, ref, actorHashString, intervalStart) 
              //if (i == 0)
                //mySuccessor = ref                
              break
            } 
            //When all are done, send updated finger table to the newly joined node. 
            i = i + 1
          }
          if (fingerNodeReturnCount == m) //The finger table is full
            joiningNodeRef ! neighborList(newFingerTable)
        }
      }
      else if (lookupType.equals("lookup"))
      {
        //We need to keep count and print the aggregate data at the end of this. After 10 requests the actor stops requesting.
        //However, the actor would still be responsible for things (Like helping others query). 
        totalHopCount = totalHopCount + hopCount
        requestReturnCount = requestReturnCount + 1
        if (requestReturnCount == 10)
        {
          println ("NODE ID: " + selfID + " Average Hop count = " + (totalHopCount/numRequests))
          bootstrap ! "over"
        }
      }
      else if (lookupType.equals("update"))
      {
        //updating other nodes
        //Tell the responding node to update its finger table
        sender ! updateFingerTable(selfID)
      }
    }
    case initFingerTable(id:Int) => {
      //Check if ID Is a valid value and call init_finger_table on this actor
      init_finger_table(id)
    }
    
    case neighborList(ft:Array[(Int, ActorRef, String, String)]) => {
      var i = 0
      while (i < m)
      {
        fingerTable(i) = ft(i)
      }
    }
    case updateFingerTable(id:Int) => {
      update_finger_table(id, sender)
    }    
  }
  
  def getHashKeyForString(inStr:String):String = { 
    //Function Returns the hash value for a given input string  
    val hash = MessageDigest.getInstance("SHA-1").digest(inStr.getBytes)
    val hashStr:String = (hash.map("%02X" format _)).mkString
    return hashStr
  }

  //-------------------------------------------------------------------------------------------------------//
  //  Chord API Functions                                                                                  //
  //-------------------------------------------------------------------------------------------------------//
  
  def find_successor(key:String, requestingActor:ActorRef, lookupType:String, hopCount:Int)= {
    //Lookup on this actor has been initiated
    //This function is the entry point of every lookup, on every actor, for anything
    //println (selfID + " : Lookup : " + key) 
    find_predecessor(key, requestingActor, lookupType, hopCount)
  }

  def find_predecessor(key:String, requestingActor:ActorRef, lookupType:String, hopCount:Int) = {
    //Am I the predecessor. If so reply to the requesting actor that I am
    //Compare the hash keys here
    //var (id:Int, ref:ActorRef, actorHashString:String, intervalStart:String) = fingerTable(0)
    if (key > getHashKeyForString(selfID.toString()) && key <= getHashKeyForString((mySuccessor._4).toString()))
    {
      //I am the predecessor
      //println ("Responsible actor found")
      //Notify the requesting actor
      if (lookupType.equals("update"))
      {
        requestingActor ! notifyRequestingActor (key, selfID, lookupType, hopCount)
      }
      else
        requestingActor ! notifyRequestingActor (key, fingerTable(0)._1, lookupType, hopCount)
    }
    else
    {
      var count = hopCount + 1
      //Find the closest neighbor and ask that guy to run the lookup
      closest_preceding_finger(key, requestingActor, lookupType, count)
    }
  }
  
  def closest_preceding_finger(key:String, requestingActor:ActorRef, lookupType:String, hopCount:Int){
    //Amongst all the actors in the finger table, find the one that is closest to the key and ask that guy to start lookup
    //println ("Closest preceding finger  " + hopCount)
    var nodeFound:Boolean = false
    val fingerTableLen:Int = fingerTable.size - 1 
    var i:Int = fingerTableLen
    for (i <- m-1 to 0)
    {
      //var (id:Int, ref:ActorRef, actorHashString:String, intervalStart:String) = fingerTable(i)
      if (getHashKeyForString((fingerTable(i)._1).toString()) < key)
      {
        nodeFound = true
        //We have found the closest neighbor 
        //Ask that guy to carry on with the lookup
        fingerTable(i)._2 ! initiateLookup(key, requestingActor, lookupType, hopCount)
      }
      
      //i = i - 1 
    }
    if (nodeFound == false)
    {
      //println ("here")
      fingerTable(m-1)._2 ! initiateLookup(key, requestingActor, lookupType, hopCount)
    }
  }
  def join(ref:ActorRef){
    if (ref != null)
    {
      //set the identifiers in our finger table now
      //ask this node to initiate our finger table
      ref ! initFingerTable(selfID)
      //Tell others that we have joined the table
    }
    else 
    {
      var selfHash = getHashKeyForString(selfID.toString)
      var i = 0
      var a = 0
      var actorRef:ActorRef = null
      val dur1 = Duration.create(1000, scala.concurrent.duration.MILLISECONDS);
      while (i < m)
      {
        a = (selfID + math.pow(2, i).toInt)
        implicit val timeout = Timeout(dur1)
        //val actorRef = Await.result(context.actorSelection("/user/").resolveOne(), timeout.duration)
        fingerTable(i) = (a, actorList(i), getHashKeyForString(a.toString), getHashKeyForString(a.toString))
        if ((fingerTable(selfID + 1) != null))
        {
              mySuccessor = fingerTable(selfID + 1)
        }
        else
          mySuccessor = fingerTable(0)
        myPredecessor = self
        i = i + 1
      }
    }
    //println ("Join Done" + selfID)
    bootstrap ! joined (selfID)
  }
  
  def init_finger_table(nodeid:Int){
    //println ("Init finger table " + selfID)
    var i = 0
    var a:Int = 0
    var intervalStart:String = null
    joiningNodeID = nodeid
    joiningNodeRef = sender
    
    while (i < m)
    {
      a = nodeid + math.pow(2, i).toInt
      intervalStart = getHashKeyForString(a.toString())
      var newFingerTableEntry:(Int,ActorRef,String,String) = (i, null, null, intervalStart) 
      //Add to finger table
      //tuples are immutable by definition, so we need to replace the existing tuple with a new one, hence the mutableList
      //For now we are adding the tuple we have right now. When the lookup comes back we can just replace the values(by replacing the tuple)
      newFingerTable(i) = newFingerTableEntry
      //find_successor(intervalStart, self, false)
      i = i + 1
    }
    i = 0
    while (i < m)
    {
      val (id, ref, str, intervalStr) = newFingerTable(i)
      find_successor(intervalStart, self, "newnode", 1)
    }
  }
  
  def update_others(){
    var i = 0
    var a = 0
    while (i < m)
    {
      a = math.abs(selfID - (math.pow(2, i).toInt))
      find_predecessor(getHashKeyForString(a.toString),  self, "update", i)
    } 
  }
  
  def update_finger_table(id:Int, senderRef:ActorRef) {
    val hashStr:String = getHashKeyForString(id.toString)
    var i = 0
    var intervalStr:String = null
    breakable{
      while (i < m)
      {
        var (id:Int, ref:ActorRef, actorHashString:String, intervalStart:String) = fingerTable(i)
        if (hashStr < actorHashString )
        {
         intervalStr = intervalStart 
          break
        }
        i = i + 1
      }
    }
    var firstPart:Array[(Int, ActorRef, String, String)] = fingerTable.take(i)
    var secondPart: Array[(Int, ActorRef, String, String)] = fingerTable.drop(i)
    //firstPart   ((id, senderRef, hashStr, intervalStr))
    fingerTable = firstPart ++ secondPart
  }
  
}

class bootstrap extends Actor {
  var totalNumNodes:Int = -1
  var numRequestPerNode:Int = 0
  var allActors:List[ActorRef] = Nil
  val nodeSystem = ActorSystem("NodeSystem")
  var joinedCount = 0
  var joinedNodeID:Int = -1
  var finishedCount = 0
  
  def receive = {
    case start (numNodes:Int, numRequests:Int) => {
      //println ("Starting system")
      totalNumNodes = numNodes
      numRequestPerNode = numRequests
      var i = 0
    //println (realNumNodes)
      while (i < numNodes ) //Will setup numNodes
      {
        allActors ::= nodeSystem.actorOf(Props[node], "node" + i)

        i = i + 1
      }
       i = 1
       allActors(0) ! init (0, numRequests, numNodes, allActors)
       Thread.sleep(100)
       while (i < numNodes)
       {
         allActors(i) ! init(i, numRequests, numNodes, allActors)
         i = i + 1
       }
      //Actor 0 starts on its own. Rest will have to start by calling join on their i-1
      //We can either setup one actor and then set the subsequent actors by using join method of that one actor
      //We can setup numNodes actors here and just start by lookup
    }
    case joined(id:Int) => {
      joinedCount = joinedCount + 1
      
      //println (joinedCount + "   " + totalNumNodes)
      if (joinedCount == totalNumNodes)
      {
        allActors.foreach { x => x ! "start" }
      }
     // else if (id != allActors.size -1)
       // initiateJoin(id)
    }
    case "over" => {
      finishedCount = finishedCount + 1
      println (finishedCount + "   " + totalNumNodes)
      if (finishedCount == totalNumNodes)
      {
        System.exit(0)
      }
    }
    
  }
  def initiateJoin(id:Int) {
    var newid = id + 1
    //println ("Initiate join" + id + " " + newid)
    allActors(newid) ! join(allActors((id)))
  }
  
} 

object project3 {
  def main (args: Array[String]) {
    if (args.length != 2) //incorrect number of arguments
    {
      println ("Usage: project3 <numNodes> <numRequests>")
      System.exit(0)
    }
    
    val numNodes = args(0).toInt
    val numRequests = args(1).toInt
    
    val bootstrapSys = ActorSystem("ManagerSystem")
    val bootstrapMan = bootstrapSys.actorOf(Props[bootstrap], "StarterNode")
    
    bootstrapMan ! start (numNodes, numRequests)

  } 
} 