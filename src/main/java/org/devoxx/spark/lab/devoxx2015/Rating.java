package org.devoxx.spark.lab.devoxx2015;

import java.io.Serializable;
import java.time.LocalDateTime;

// Spark nécessite des structures sérializables !!
public class Rating implements Serializable {

    public int rating;
    public long movie;
    public long user;
    public LocalDateTime timestamp;

    public Rating(long user, long movie, int rating, LocalDateTime timestamp) {
        this.user = user;
        this.movie = movie;
        this.rating = rating;
        this.timestamp = timestamp;
    }
}
