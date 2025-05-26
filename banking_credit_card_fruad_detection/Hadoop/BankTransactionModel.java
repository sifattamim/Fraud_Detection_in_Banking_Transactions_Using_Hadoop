package com.fraud.model;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A data class representing individual credit card transaction details.
 */
public class CardTransaction implements Serializable {

    private static final long serialVersionUID = 42L;

    @JsonProperty("card_id")
    private Long cardId;

    @JsonProperty("member_id")
    private Long memberId;

    @JsonProperty("amount")
    private Double amount;

    @JsonProperty("pos_id")
    private Long terminalId;

    @JsonProperty("postcode")
    private Integer postalCode;

    @JsonProperty("transaction_dt")
    private String timestamp;

    public CardTransaction() {
        // default constructor
    }

    public CardTransaction(long cardId, long memberId, double amount, long terminalId, int postalCode, String timestamp) {
        this.cardId = cardId;
        this.memberId = memberId;
        this.amount = amount;
        this.terminalId = terminalId;
        this.postalCode = postalCode;
        this.timestamp = timestamp;
    }

    // Getters
    public Long getCardId() {
        return cardId;
    }

    public Long getMemberId() {
        return memberId;
    }

    public Double getAmount() {
        return amount;
    }

    public Long getTerminalId() {
        return terminalId;
    }

    public Integer getPostalCode() {
        return postalCode;
    }

    public String getTimestamp() {
        return timestamp;
    }

    // Setters
    public void setCardId(long cardId) {
        this.cardId = cardId;
    }

    public void setMemberId(long memberId) {
        this.memberId = memberId;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public void setTerminalId(long terminalId) {
        this.terminalId = terminalId;
    }

    public void setPostalCode(int postalCode) {
        this.postalCode = postalCode;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return cardId + "," + amount + "," + memberId + "," + terminalId + "," + postalCode + "," + timestamp;
    }
}
