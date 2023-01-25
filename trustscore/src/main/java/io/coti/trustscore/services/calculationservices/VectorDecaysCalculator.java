package io.coti.trustscore.services.calculationservices;

import io.coti.trustscore.config.rules.EventScore;
import io.coti.trustscore.data.events.BalanceCountAndContribution;
import io.coti.trustscore.utils.DatesCalculation;
import org.apache.commons.lang3.tuple.MutablePair;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.coti.trustscore.utils.DatesCalculation.calculateDaysDiffBetweenDates;

public class VectorDecaysCalculator<T extends EventScore> {
    private final DecayCalculator decayCalculator;
    private final Map<T, Map<Date, BalanceCountAndContribution>> eventScoresToDatesScoreMap;


    public VectorDecaysCalculator(Map<T, Map<Date, BalanceCountAndContribution>> eventScoresToDatesScoreMap) {
        this.eventScoresToDatesScoreMap = eventScoresToDatesScoreMap;
        this.decayCalculator = new DecayCalculator<>();
    }

    public Map<T, Double> calculateDatesVectorDecays(Date lastUpdate) {
        Map<T, Double> eventScoresToTotalNewValueMap = new ConcurrentHashMap<>();

        // Iterate on every event
        for (Map.Entry<T, Map<Date, BalanceCountAndContribution>> eventScoresToDatesScoreMapEntry : eventScoresToDatesScoreMap.entrySet()) {
            T eventScore = eventScoresToDatesScoreMapEntry.getKey();
            double sumEventScore = 0;
            Map<Date, BalanceCountAndContribution> dayToScoreMap = eventScoresToDatesScoreMapEntry.getValue();

            // Iterate on every date
            for (Map.Entry<Date, BalanceCountAndContribution> dayToScoreMapEntry : dayToScoreMap.entrySet()) {
                int numberOfDecays = calculateDaysDiffBetweenDates(DatesCalculation.setDateOnBeginningOfDay(lastUpdate),
                        DatesCalculation.setDateOnBeginningOfDay(new Date()));
                double currentDailyScore = dayToScoreMapEntry.getValue().getContribution();

                MutablePair scoreAfterDecay =
                        decayCalculator.calculateEntry(new EventDecay(eventScore, currentDailyScore), numberOfDecays);
                double decayedCurrentDailyScore = (double) scoreAfterDecay.getValue();
                dayToScoreMapEntry.setValue(new BalanceCountAndContribution(dayToScoreMapEntry.getValue().getCount(),
                        decayedCurrentDailyScore));
                sumEventScore += decayedCurrentDailyScore;
            }
            eventScoresToTotalNewValueMap.put(eventScore, sumEventScore);
        }
        return eventScoresToTotalNewValueMap;
    }
}
