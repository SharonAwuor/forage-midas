package com.jpmc.midascore.entity;

import jakarta.persistence.*;

@Entity
public class TransactionRecord {

    @Id
    @GeneratedValue
    private Long id;

    @ManyToOne
    private UserRecord sender;

    @ManyToOne
    private UserRecord recipient;

    private float amount;

    protected TransactionRecord() {}

    public TransactionRecord(UserRecord sender,
                             UserRecord recipient,
                             float amount) {
        this.sender = sender;
        this.recipient = recipient;
        this.amount = amount;
    }
}