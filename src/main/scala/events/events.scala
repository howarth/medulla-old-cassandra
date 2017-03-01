package main.scala.events
import main.scala.core._


/*
abstract class PossiblePartialMatch(matchedSoFar : MatchedStimulusEventList,
                                    stillToMatch : MatchedStimulusEventList,
                                    score : Int,
                                    nextMatcher : EventMatcher)
class PossibleMatch(matches : MatchedStimulusEventList)

abstract class ValidateSetOfMatches(matches : List[PossibleMatch])

trait EventMatcher {
  def matchEvents(rest : TriggerEventList) : List[PossiblePartialMatch]
}


class EventFixerWithKnownTiming(expectedStimulusEvents : StimulusEventList) extends EventMatcher{
  def matchEvents(observed : TriggerEventList) : MatchedStimulusEventList = throw new NotImplementedError()
}

class EventMatcherWithTriggerToStimulus(triggerToStimulus : Map[Int, Stimulus]) extends EventMatcher{
  def matchEvents(observed : TriggerEventList) : MatchedStimulusEventList = throw new NotImplementedError()
}

*/