package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.stereotype.Component;

@Component
public class DatabaseConduit {

    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;

    public DatabaseConduit(UserRepository userRepository,
                           TransactionRepository transactionRepository) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
    }

    public void save(UserRecord userRecord) {
        userRepository.save(userRecord);
    }

    public UserRecord findUser(long id) {
        return userRepository.findById(id);
    }

    public UserRecord findUserByName(String name) {
        return userRepository.findByName(name);
    }

    public void saveTransaction(TransactionRecord transaction) {
        transactionRepository.save(transaction);
    }

    public float getBalance(String username) {
        UserRecord user = userRepository.findByName(username);
        return user.getBalance();
    }

    public long transactionCount() {
        return transactionRepository.count();
    }

    public Iterable<UserRecord> getAllUsers() {
        return userRepository.findAll();
    }
}