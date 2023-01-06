package com.dao.impl;

import com.dao.UserDao;

public class UserDaoImpl implements UserDao {

    public UserDaoImpl() {
        System.out.println("UserDaoImpl创建。。。");
    }

    public void save() {
        System.out.println("save running ....");
    }
}
