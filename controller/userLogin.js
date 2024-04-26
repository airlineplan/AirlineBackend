const nodemailer = require("nodemailer");
const { google } = require('googleapis');
const User = require("../model/userSchema");
const jwt = require("jsonwebtoken");
const bcrypt = require("bcryptjs");
const config = require("../config/config");
const Otp = require("../model/otp");
const { text } = require("body-parser");
exports.createUser = async (req, res) => {
  const { firstName, lastName, email, password } = req.body;

  const existingUser = await User.findOne({ email: email });

  if (existingUser) {
    return res.status(400).send("User already exists");
  }
  const salt = await bcrypt.genSalt(10);
  const hashedPassword = await bcrypt.hash(password, salt);

  const user = new User({
    firstName,
    lastName,
    email,
    password: hashedPassword,
  });

  try {
    const savedUser = await user.save();
    res.send(savedUser);
  } catch (err) {
    res.status(400).send(err);
  }
};

exports.loginUser = async (req, res) => {
  try {
    console.log(req.body);
    const { email, password } = req.body;

    const user = await User.findOne({ email: email });
    if (!user) {
      return res.status(400).json({ error: "Invalid credentials" });
    }

    const isMatch = await bcrypt.compare(password, user.password);
    if (!isMatch) {
      return res.status(400).json({ error: "Invalid credentials" });
    }

    const payload = {
      id: user.id,
      email: user.email,
    };

    jwt.sign(payload, config.secret, { expiresIn: "7h" }, (err, token) => {
      if (err) throw err;
      console.log(token, "token ");
      res.json({ message: "Login successful", token });
    });
  } catch (error) {
    console.error(error.message);
    res.status(500).json({ error: "Server error" });
  }
};
exports.changePassword = async (req, res) => {
  try {
    const data = await Otp.findOne({
      email: req.body.email,
      code: req.body.otpCode,
    });

    const response = {};
    if (data) {
      let currentTime = new Date().getTime();
      let diff = data.expireIn - currentTime;
      if (diff < 0) {
        response.message = "Token Expire";
        response.statusText = "error";
      } else {
        const salt = await bcrypt.genSalt(10);
        const hashedPassword = await bcrypt.hash(req.body.password, salt);

        const user = await User.findOne({ email: req.body.email });
        if (!user) {
          response.message = "User not found";
          response.statusText = "error";
        } else {
          user.password = hashedPassword;
          await user.save();
          response.message = "Password Changed Successfully";
          response.statusText = "Success";
        }
      }
    } else {
      response.message = "Invalid OTP";
      response.statusText = "error";
    }
    res.status(200).json(response);
  } catch (error) {
    console.error(error.message);
    res.status(500).json({ error: "Server error" });
  }
};

exports.sendEmail = async (req, res) => {
  try {
    console.log({ email: req.body.email });
    const data = await User.findOne({ email: req.body.email });

    const responseType = {};

    if (data) {
      const otpcode = Math.floor(Math.random() * 9000) + 1000;
      const otpData = new Otp({
        email: req.body.email,
        code: otpcode,
        expireIn: new Date().getTime() + 300 * 1000,
      });

      await otpData.save();

      await sendEmail(req.body.email, otpcode);

      responseType.statusText = "Success";
      responseType.message = "Please Check Your Email Id";
    } else {
      responseType.statusText = "error";
      responseType.message = "Email id not Exist";
    }

    res.status(200).json(responseType);
  } catch (error) {
    console.error(error);
    res.status(500).json({ statusText: "error", message: "An error occurred" });
  }
};

const sendEmail = async (email, otp) => {
  try {
    
    // Create a Nodemailer transporter
    const transporter = nodemailer.createTransport({
      service: 'Gmail', // Change this to your email service provider
      auth: {
        user: 'admin@airlineplan.com', // Replace with your email address
        pass: 'pfns kyja srcb rxwj', // Replace with your email password
      },
    });

    const mailOptions = {
      from: "admin@airlineplan.com",
      to: email,
      subject: "AirlinePlan OTP ",
      text: `Your OTP code is: ${otp}`,
    };

    const info = await transporter.sendMail(mailOptions);
    console.log("Email sent:", info.response);
  } catch (error) {
    console.error("Error sending email:", error);
  }
};

exports.sendContactEmail = async (req, res) => {
  try {
    const { name, email, subject, message } = req.body;

    // Create a Nodemailer transporter
    const transporter = nodemailer.createTransport({
      service: 'Gmail', // Change this to your email service provider
      auth: {
        user: 'admin@airlineplan.com', // Replace with your email address
        pass: 'pfns kyja srcb rxwj', // Replace with your email password
      },
    });

    // Email content
    const mailOptions = {
      from: 'admin@airlineplan.com', // Sender email address
      to: 'admin@airlineplan.com', // Receiver email address
      subject: subject,
      text: `Name: ${name}\nEmail: ${email}\nMessage: ${message}`,
    };

    // Send email
    await transporter.sendMail(mailOptions);
    res.status(200).json({ message: 'Email sent successfully' });
  } catch (error) {
    console.error('Error sending email:', error);
    res.status(500).json({ message: 'Error sending email' });
  }

};

