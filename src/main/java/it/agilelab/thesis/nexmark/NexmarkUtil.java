package it.agilelab.thesis.nexmark;

import it.agilelab.thesis.nexmark.model.Auction;
import it.agilelab.thesis.nexmark.model.Bid;
import it.agilelab.thesis.nexmark.model.NextEvent;
import it.agilelab.thesis.nexmark.model.Person;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public final class NexmarkUtil {
    private static final Logger LOGGER = LogManager.getLogger(NexmarkUtil.class);

    private NexmarkUtil() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * This method returns the topic name based on the event type.
     *
     * @param element the event
     * @return the topic name
     */
    public static String selectTopic(final NextEvent element) {
        switch (element.getActualEvent().getEventType()) {
            case AUCTION:
                return "auctions";
            case BID:
                return "bids";
            case PERSON:
                return "people";
            default:
                throw new IllegalArgumentException("Unknown event type: " + element.getActualEvent().getEventType());
        }
    }

    /**
     * This method returns the key for the ProducerRecord based on the event type.
     * The key is the id of the event.
     *
     * @param element the event
     * @return the key of the ProducerRecord based on the event type
     */
    public static String selectKey(final NextEvent element) {
        switch (element.getActualEvent().getEventType()) {
            case AUCTION:
                Auction auction = (Auction) element.getActualEvent().getActualEvent();
                return String.valueOf(auction.getId());
            case BID:
                Bid bid = (Bid) element.getActualEvent().getActualEvent();
                return String.valueOf(bid.getBidder());
            case PERSON:
                Person person = (Person) element.getActualEvent().getActualEvent();
                return String.valueOf(person.getId());
            default:
                throw new IllegalArgumentException("Unknown event type: " + element.getActualEvent().getEventType());
        }
    }

    /**
     * This method returns the logger.
     *
     * @return the logger.
     */
    public static Logger getLogger() {
        return LOGGER;
    }

    /**
     * This method reads the query file and returns a list of queries.
     *
     * @param filePath the path of the query file
     * @return a list of queries
     * @throws IOException if something goes wrong during the reading of the file
     */
    public static List<String> readQueryFileAsString(final String filePath) throws IOException {
        String fileContent = new String(Files.readAllBytes(Paths.get(filePath)));
        List<String> queryList = new ArrayList<>(List.of(fileContent.split("(?<=;)")));
        if (queryList.get(queryList.size() - 1).isEmpty() || !queryList.get(queryList.size() - 1).contains(";")) {
            queryList.remove(queryList.size() - 1);
        }
        return queryList;
    }
}
