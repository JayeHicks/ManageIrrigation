/*******************************************************************************
 * Jaye Hicks, 2020 
 * 
 * Obligatory legal disclaimer:
 *   You are free to use this source code (this file and all other files 
 *   referenced in this file) "AS IS" WITHOUT WARRANTY OF ANY KIND, EITHER 
 *   EXPRESSED OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
 *   WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE 
 *   ENTIRE RISK AS TO THE QUALITY AND PERFORMANCE OF THIS SOURCE CODE IS WITH 
 *   YOU.  SHOULD THE SOURCE CODE PROVE DEFECTIVE, YOU ASSUME THE COST OF ALL 
 *   NECESSARY SERVICING, REPAIR OR CORRECTION. See the GNU GENERAL PUBLIC 
 *   LICENSE Version 3, 29 June 2007 for more details.
 * 
 * Goal of Hardware and Software:
 *   Self-contained, battery powered (solar panel battery charging), WiFi
 *   connection to gain Internet access, regularly scheduled transmission 
 *   of sensor values for: power provided by battery, readings of (3) 
 *   soil moisture sensors, temperature, and humidity if DHT22 configured to 
 *   an Internet endpoint.
 * 
 * Hardware Configuration:
 *   "Vinduino R3 Sensor Station Platform" on-board components include
 *     ATMega 328P
 *     PCF8563 Real Time Clock
 *     I2C is used to communicate between ATMega and PCF
 *   Sensor Configuration (choice between one or zero of the following):
 *     DHT22 (humidity and temperature readings)
 *        -OR-
 *     DS18B20 (temperature readings only)
 *       Uses OneWire to communicate between ATMega and DS18B20
 *     (3) soil moisture sensors (Davis Instruments, Vantage Pro2)
 *   ESP2866-01 WiFi Serial Transceiver module
 *     using factory firmware (i.e., AT commands)
 *
 * Arduino IDE Settings:
 *    Board: Arduino Pro or Pro Mini
 *    Processor: ATMega328P (3.3v 8MHz)
 *    Dependencies: 
 *      Arduino IDE will automatically provide these files to your sketch compile
 *        Wire.h 
 *        math.h
 *      Colocating remaining files with the sketch .ino file is recommended.  
 *      You will most likely need to make minor edits to remove relative paths
 *      for included files.  In particular, the OneWire.h recommended below
 *      requires two additional header files, OneWire_direct_gpio.h and 
 *      OneWire_direct_regtype.h, and attempts to include them with a relative 
 *      path. So if you colocate all files with the .ino sketch file you will 
 *      need to remove the "util/" prefix from the include statements for these
 *      two files.
 *
 *      The non-Arduino-provided-files and where to source them:
 *        For the DHT22 sensor
 *          DHT.h, DHT.cpp            
 *           (https://github.com/adafruit/DHT-sensor-library)
 *          LowPower.h, LowPower.cpp  
 *           (https://github.com/lowpowerlab/lowpower)
 *        For DS18B20 sensor
 *          DallasTempearture.h, DallasTemperature.cpp
 *            (https://github.com/milesburton/Arduino-Temperature-Control-Library)
 *          OneWire.h, OneWire.cpp, OneWire_direct_gpio.h, OneWire_direct_regtype.h
 *            (https://github.com/PaulStoffregen/OneWire)
 *          LowPower.h, LowPower.cpp  
 *           (https://github.com/lowpowerlab/lowpower)
 *
 * This Arduino IDE sketch is based upon prior work from:
 *   Reinier van der Lee
 *   www.vanderleevineyard.com
 *   GitHub repo: https://github.com/ReiniervdL/Vinduino
 *
 *********************************************************************************
 * Lonesome Vine checklist for deploying a circuit board to production (i.e., not test):
 *   - uncomment the line "#define PRODUCTION (i.e., ensure "//" does not start line)
 *   - uncomment the line "#define BATTERY_POWER" (i.e., ensure "//" does not start line)
 *   - comment the line "#define USING_DHT" (i.e., ensure "//" starts the line)
 *   - set VINEYARD_SENSOR_STATION_LOCATION (i.e., insert 4 char code between double quotes)
 *   - search for "SSID[]" and ensure it is set to the vineyard wifi network name
 *   - search for "PASS[]" and ensure it is set to the vineyard wifi network password
 *   - search for "timer_setting_1" and ensure the preceding code on that line 
 *     reads "Wire.write(PCF_8563_ENABLE_TIMER_MINS);"
 *   - search for "timer_setting_2" and ensure the preceding code on that line
 *     reads "Wire.write(PCF_8563_SET_TIMER_60_MIN);"
 ********************************************************************************/
//#define PRODUCTION
//#define  USING_DHT              // uncomment for DHT config, comment for DS18B20 config
#define  BATTERY_POWER            // uncomment for battery, comment for programming cable

#include <Wire.h>                 // I2C comms between ATMega and PCF RealTimeClock
#include <math.h>                 // conversion equation from resistance to %
#include "LowPower.h"             // used for sleep mode functionality
#ifdef USING_DHT
  #include "DHT.h"                // DHT22 temperature and humidity sensor
#else
  #include "DallasTemperature.h"  // DS18B20 temperature sensor
  #include "OneWire.h"            // OneWire comms between ATMega and DS18B20
#endif

#ifdef USING_DHT
const int         DHT22_PIN = 12;
const bool        IN_FAREN = true;
DHT               theDHT(DHT22_PIN, DHT22);    // DHT sensor object
#else
const int         ONE_WIRE_BUS = 12; 
OneWire           ourWire(ONE_WIRE_BUS);
DallasTemperature theDS18B20(&ourWire);
#endif

/******************************************************************************************
 * The location of a vineyard sensor station is a compound key.  Using 'd2n' as an example
 * the location key designates block "d", row "2" within block "d", "n" side (i.e., north)
 ******************************************************************************************/
char VINEYARD_SENSOR_STATION_LOCATION[] = "TEST";

/******************************************************************************************
 * Soil Moisture Sensors:
 * To determine soil moisture, multiple measurements of resistance will be taken for each 
 * individual sensor; these resistance values will be averaged to determine the moisture
 * level for a sensor.  Soil moisture readings will be conducted once per 24 hour period.  
 * Soil moisture readings will be sent every time the sensor station sends data to the back
 * end but these readings will be a repeat of the values obtained from the single daily read
 * until 24 hours elapses and the soil moisture readings are updated.
 ******************************************************************************************/
const int  SELECT_SM_SENS_1  = 1;	           // used in a switch/case statement
const int  SELECT_SM_SENS_2  = 2;              // used in a switch/case statement
const int  SELECT_SM_SENS_3  = 3;              // used in a switch/case statement
const int  SMS_NUM_READS     = 5;              // # of readings per sensor per single read event
long       soilSensorReadings[SMS_NUM_READS];  // readings from single read event for 1 sensor
int        indexToAddReading = 0;              // index through soilSensorReadings[]

char strSMS1[32];                              // sms measured every 24 hours; last read for sms1
char strSMS2[32];                              // sms measured every 24 hours; last read for sms2
char strSMS3[32];                              // sms measured every 24 hours; last read for sms3
int  hour_count = 0;                           // keep track of hours elapsed since last sms read


/******************************************************************************************
 * Comms: WiFi / Internet Communication
 ******************************************************************************************/
char     SSID[]           = "XYZ";
char     PASS[]           = "ABC";
char     THING_SPEAK_IP[] = "999.999.999.999";             // www.thingspeak.com
char     GET_COMMAND[]    = "GET /update";
#ifdef PRODUCTION
char     TS_API_KEY[]     = "?api_key=1234567890123456";   // production; unique per channel
#else
char     TS_API_KEY[]     = "?api_key=123456789012345678";   // test; unique per channel
#endif

/*******************************************************************************************
 * ditial pin assignments
 *******************************************************************************************/
const int  WAKE_UP_PIN                = 2;     // pin used by Sleep/Wake functionality
const int  WIFI_PIN                   = 13;    // pin used by WiFi and Aux Power
const int  SM_SENS_DRIVER_A           = 5;	   // pin used by soil moisture sensor
const int  SM_SENS_DRIVER_B           = 6;     // pin used by soil moisture sensor
const int  MUX_ENABLE                 = 7;     // pin used by soil moisture sensor
const int  MUX_INPUT_A                = 8;     // pin used by soil moisture sensor
const int  MUX_INPUT_B                = 9;     // pin used by soil moisture sensor

/*******************************************************************************************
 * PCF 8563 Real Time Clock: chip address, register addresses, timer configuration
 *******************************************************************************************/
const int  PCF_8563_ADDRESS           = 0x51;
const int  PCF_8563_REG2_ADDR         = 0x01;
const int  PCF_8563_REG2_ALARM_BIT    = 0x01;
const int  PCF_8563_TIMER_CNTL_ADDR   = 0x0E;
const int  PCF_8563_TIMER_VAL_ADDR    = 0x0F;

// to use timer in seconds, config for seconds then set number of seconds desired
const int  PCF_8563_ENABLE_TIMER_SECS = 0x82;   // config clock to 1 Hz to get timer
const int  PCF_8563_SET_TIMER_15_SEC  = 0x0F;   // to operate in seconds
const int  PCF_8563_SET_TIMER_60_SEC  = 0x3C;  

// to use timer in minutes, config for minutes then set number of minutes desired
const int  PCF_8563_ENABLE_TIMER_MINS = 0x83;   // config clock 1/60 Hz to get timer
const int  PCF_8563_SET_TIMER_15_MIN  = 0x0F;   // to operate in minutes
const int  PCF_8563_SET_TIMER_60_MIN  = 0x3C;
const int  PCF_8563_SET_TIMER_2_HR    = 0x78;
const int  PCF_8563_SET_TIMER_4_HR    = 0xF0;


/************************************************************************** 
 * Per Arduino IDE, at micro controller boot time this function will be 
 * executed once after which execution control will be passed to a function 
 * named loop() that will executed repdeatedly (i.e., indefinitely).
 **************************************************************************/
void setup() 
{
  hour_count = 0;
  
  // Serial monitor used to issue AT commands to WiFi chip
  Serial.begin(115200); 

// NOTE: DS18B20 initialization is handled by object creation 
#ifdef USING_DHT
  theDHT.begin();   
#endif 

  // initialize I2C comms
  Wire.begin(); 
  
  // To enable WiFi chip, set this pin to HIGH
  pinMode(WIFI_PIN, OUTPUT);

  // configure pin for "wake up" from sleep mode
  pinMode(WAKE_UP_PIN, INPUT); 

  // soil moisture: pins used as high impedence inputs to drive a sensor
  pinMode(SM_SENS_DRIVER_A, INPUT);    
  pinMode(SM_SENS_DRIVER_B, INPUT); 
  
  // soil moisture: setting pin to LOW will enables the Mux switches 
  pinMode(MUX_ENABLE, OUTPUT); 
  
  // soil moisture: use HIGH / LOW settings across 2 pins to select a sensor
  pinMode(MUX_INPUT_A, OUTPUT);
  pinMode(MUX_INPUT_B, OUTPUT);
    
  // set up a timer for regular, ongoing wake ups from sleep mode
  setTimerPCF8563();
}

/*************************************************************************
 * Per Arduino IDE, at micro controller boot time this funciton will
 * receive execution control after the function setup() has finished 
 * processing.  This function will execute indefinitley (i.e., when execution
 * reaches the bottom of the function the function is called again and this
 * repeats indefinitely.
 *
 * Algorithm Overview: 
 *   Attach an interrupt which is associated with a timer
 *   Power down to conserve battery (solar panel charging will still occur)
 *   Timer expires and triggers execution of the associated interrupt
 *   Detach interrupt while reading / sending sensor data
 *   Go back to step 1 (i.e., attach an interrupt) 
 *************************************************************************/
void loop() 
{  
  attachInterrupt(0, wakeUp, LOW);
    
  // enter sleep mode (ADC and BOD modules disabled)
  LowPower.powerDown(SLEEP_FOREVER, ADC_OFF, BOD_OFF); 

  //******************************************************************
  // After the PCF timer goes off and wakes up the ATMega controller, 
  // the function wakeUp() is executed and then execution flow resumes
  // here (i.e., call to detachInterrupt() immediately below)
  //******************************************************************
  
  // disable any further "wake ups" while collecting and sending data
  detachInterrupt(0); 
   
  // if alarm set, reset
  checkPCF8563Alarm();
  delay(1000); 

  joinWirelessNetwork();
	
  readSensorsAndSendData(); 

  //turn off WiFi chip
  digitalWrite(WIFI_PIN, LOW);
  delay(1000);
}

#ifdef BATTERY_POWER      
/*******************************************************************
 * Power:
 * Use this economical (from a power consumption perspective) code 
 * when the Vinduino board is powered by the battery.
 *
 * Read the available voltage being supplied by the board.  This 
 * algorithm will only work when the Vinduino Sensor Station board is
 * powered by the battery.  When the board is powered by the USB
 * programming cable (i.e., board is powered/tethered to 
 * your laptop) this algorithm will return value less than 1. 
 *******************************************************************/
float readVcc() 
{
  float Vcc = analogRead(3) * 0.00647;
  return(Vcc);  
}

#else
	
/*******************************************************************
 * Power: 
 * Use this code when the Vinduino board is powered by the USB
 * programming cable. Do not use when powered by battery as a much
 * more efficient (i.e., power consumption) algorithm is available.
 *
 * Read the available voltage being supplied by the board.  Even though
 * this algorithm works when the Vinduino Sensor Station board is powered
 * by the USB programming cable (i.e., board is powered/tethered to 
 * your laptop) or by the battery, use the one-liner algorithm as it
 * more economical from a power consumption perspective. 
 *******************************************************************/
float readVcc() 
{ 
  float Vcc;
  long measurement;
  
  // read 1.1V reference against AVcc 
  ADMUX = _BV(REFS0) | _BV(MUX3) | _BV(MUX2) | _BV(MUX1);

  // allow Vref time to settle 
  delay(2);       

  // convert
  ADCSRA |= _BV(ADSC);                 
  
  while (bit_is_set(ADCSRA, ADSC))
  {
    // while loop used as synchronous, busy wait
  }; 
  
  measurement = ADCL; 
  measurement |= ADCH << 8; 
  
  // back-calculate AVcc in mV 
  measurement = (1125300L / measurement);       

  Vcc = measurement;
  return(Vcc / 1000); 
}
#endif

/******************************************************************************
 * read all sensors, send sensor data to Internet endpoint (www.thingspeak.com).
 * Note: soil moisture only read once per 24 hour period; last readings are
 * repeated until 24 hours elapses and another soil moisture reading is taken
 *******************************************************************************/
void readSensorsAndSendData()
{
  char strTemp[8];
  char strHumidity[8];
  char strVcc[8];
  
  //On the first hour of each 24 hour cycle take reading from all soil moisture sensors
  //and store in global variable so readings repeated with every sensor station report.
  if(hour_count == 0)
  {
    hour_count++;
	
    selectSensor(SELECT_SM_SENS_1);
    measureMoisture();
    unsigned long moist1 = averageReadings();
    sprintf(strSMS1,"%lu", moist1);

    selectSensor(SELECT_SM_SENS_2);
    measureMoisture();
    unsigned long moist2 = averageReadings();
    sprintf(strSMS2,"%lu", moist2);

    selectSensor(SELECT_SM_SENS_3);
    measureMoisture();
    unsigned long moist3 = averageReadings();
    sprintf(strSMS3,"%lu", moist3);

    delay(1000);
  }
  else if((hour_count >= 24) || (hour_count < 0))
  {
    hour_count = 0;
  }
  else
  {
    hour_count++;	  
  }

  // measure battery voltage
  float Vcc = readVcc();
  if(isnan(Vcc))
  {
    sprintf(strVcc,"%s","NAN");
  }
  else
  {
    dtostrf(Vcc,4,2,strVcc);
  }
  
  delay(1000);
  
  // get temperature reading; also humidity reading for DHT22 configuration;
  // for DS18B20 first read after boot returns temperature of +185°
#ifdef USING_DHT
  float temp = theDHT.readTemperature(IN_FAREN, false);
  float humidity = theDHT.readHumidity(false);
  
#else
  theDS18B20.requestTemperatures(); 
  float temp = (theDS18B20.getTempFByIndex(0));
  float humidity = 0;  // to indicate no humidity reading taken
#endif 
  if((isnan(temp)) || (temp == (float)185))
  {
    sprintf(strTemp,"%s","NAN");	  
  }
  else
  { 
    dtostrf(temp,4,2,strTemp);
  }
  if(isnan(humidity))
  {
    sprintf(strHumidity,"%s","NAN");
  }
  else
  {
    dtostrf(humidity,4,2,strHumidity);
  }
  delay(1000);
     
  /* a useful block print statements for debugging purposes (with programming cable)
  Serial.print("************************************ Begin: Sensor Readings");
  Serial.println("******************************************");
  Serial.print("Vcc: ");
  Serial.print(strVcc);
  Serial.print(", Temp: ");
  Serial.print(strTemp);
  Serial.print(", Hum: ");
  Serial.print(strHumidity);
  delay(1000);  // avoids potential for garbled output display on Serial monitor
  Serial.print(", SM Sensor 1: ");
  Serial.print(strSMS1);
  Serial.print(", SM Sensor 2: ");
  Serial.print(strSMS2);
  Serial.print(", SM Sensor 3: ");
  Serial.println(strSMS3);
  Serial.print("************************************** End: Sensor Readings");
  Serial.println("******************************************");
  delay (1000);
  */
  
  sendSensorData(strTemp, strHumidity, strSMS1, strSMS2, strSMS3, strVcc);
}

/**********************************************************************************
 * Soil Moisture Sensors:
 * prepare for a soil moisture reading, that will occur with a call to 
 * measureMoisture().  A specific soil moisture is selected and the Mux switches
 * are enabled. The values of two digital pins are set (i.e., HIGH or LOW) in order
 * specify a particular soil moisture sensor.
 *
 * soil moisture sensor 1 == ( (MUX_INPUT_A == LOW) && (MUX_INPUT_B == LOW))
 * soil moisture sensor 2 == ( (MUX_INPUT_A == LOW) && (MUX_INPUT_B == HIGH))
 * soil moisture sensor 3 == ( (MUX_INPUT_A == HIGH) && (MUX_INPUT_B == LOW))
 *
 * for future reference:
 * soil moisture sensor 4 == ( (MUX_INPUT_A == HIGH) && (MUX_INPUT_B == HIGH))
 *
 **********************************************************************************/
void selectSensor(int sensor_selection)
{
  switch(sensor_selection)
  {
    case SELECT_SM_SENS_1:
	  digitalWrite(MUX_INPUT_A, LOW); 
      digitalWrite(MUX_INPUT_B, LOW); 
	  break;
	case SELECT_SM_SENS_2:
	  digitalWrite(MUX_INPUT_A, LOW); 
      digitalWrite(MUX_INPUT_B, HIGH); 
	  break;
	case SELECT_SM_SENS_3:
	  digitalWrite(MUX_INPUT_A, HIGH); 
      digitalWrite(MUX_INPUT_B, LOW); 
	  break;
	default:
	  digitalWrite(MUX_INPUT_A, LOW); 
      digitalWrite(MUX_INPUT_B, LOW); 
	  break;
  }
  digitalWrite(MUX_ENABLE, LOW); 
}

/**********************************************************************************
 * Soil Moisture Sensors:
 * A prior function call was made to selectSensor() that designates which specific
 * soil moisture sensor will be read by this function call.  For the selected sensor,
 * perform reads and resistance calculations that are stored in a global array and
 * will ultimatley be averaged. 
 **********************************************************************************/
void measureMoisture()
{
  const long    KNOWN_RESISTOR = 4750;  // the value of Resistor 9 and Resistor 10 expressed in Ohms;
                                        // expressed in Ohms.  Used as ref in resistance calculation
  const int     ZERO_CALIBRATION = 95;  // to compensate for resistance added to the circuit
                                        // by the Mux switches
  long          resistance;
  unsigned long supplyVoltage;
  unsigned long sensorVoltage;
 				
  initArray();   // remove undefines on initial use or previous readings when reusing

  for (int i = 0; i < SMS_NUM_READS; i++) 
  {
    pinMode(SM_SENS_DRIVER_A, OUTPUT); 
    digitalWrite(SM_SENS_DRIVER_A, LOW);  
    digitalWrite(SM_SENS_DRIVER_A, HIGH); 
    delayMicroseconds(250);
    sensorVoltage = analogRead(0); 
    supplyVoltage = analogRead(1); 
    digitalWrite(SM_SENS_DRIVER_A, LOW); 
    pinMode(SM_SENS_DRIVER_A, INPUT);
    resistance = (KNOWN_RESISTOR * (supplyVoltage - sensorVoltage ) / 
	              sensorVoltage) - ZERO_CALIBRATION;
    addReading(resistance);
	
    delayMicroseconds(250);

    pinMode(SM_SENS_DRIVER_B, OUTPUT); 
    digitalWrite(SM_SENS_DRIVER_B, LOW);  
    digitalWrite(SM_SENS_DRIVER_B, HIGH); 
    delayMicroseconds(250);
    sensorVoltage = analogRead(1); 
    supplyVoltage = analogRead(0); 
    digitalWrite(SM_SENS_DRIVER_B, LOW); 
    pinMode(SM_SENS_DRIVER_B, INPUT);
    resistance = (KNOWN_RESISTOR * (supplyVoltage - sensorVoltage ) / 
	              sensorVoltage) - ZERO_CALIBRATION;
    addReading(resistance);
	
    delay(100);
  } 
}

/********************************************************************
 * Soil Moisture Sensors:
 * Add a single soil sensor reading to array of soil sensor readings
 ********************************************************************/
void addReading(long resistance)
{
  soilSensorReadings[indexToAddReading++] = resistance;
  
  if (indexToAddReading >= SMS_NUM_READS)
  {
    indexToAddReading = 0;
  }
}

/**************************************************************************
 * Soil Moisture Sensors:
 * Average soil moisture sensor readings.  
 ***************************************************************************/
long averageReadings()
{
  long sum = 0;
  
  for (int i = 0; i < SMS_NUM_READS; i++)
  {
    sum += soilSensorReadings[i];
  }
  
  return((long)(sum / SMS_NUM_READS));
}

/***************************************************************************
 * Soil Moisture Sensors:
 * safely reuse a global array by initialization / resetting
 ***************************************************************************/
void initArray()
{
  for (int i = 0; i < SMS_NUM_READS; i++)
  {
    soilSensorReadings[i] = 0;	  
  }
}

/***********************************************************************
 * PCF8563 Timer: 
 * The attachInterrupt() function requires a pointer to a function.  The
 * function will be executed as part of processing the interrupt event.
 ***********************************************************************/
void wakeUp()
{
  // In our particular case, no processing required here
}

/****************************************************************
 * PCF8563 Timer:
 * Retrieve the existing settings for PCF8563 control register 2
 ****************************************************************/
byte readPCF8563CntrlReg2()
{
  byte settings;
  
  Wire.beginTransmission(PCF_8563_ADDRESS);  // access PCF
  Wire.write(PCF_8563_REG2_ADDR);       // index to PCF cntrl reg 2
  Wire.endTransmission();
  Wire.requestFrom(PCF_8563_ADDRESS, PCF_8563_REG2_ALARM_BIT);
  settings = Wire.read();            // read PCF cntrl reg 2 settings
  
  return(settings);
}

/**********************************************************
 * PCF8563 Timer:
 * Turn off the alarm flag (i.e., set bit 2 to 0 in cntrl reg)
 **********************************************************/
void PCF8563AlarmOff()
{
  byte settings = readPCF8563CntrlReg2();

  // build a PCF cntrl reg 2 setting that turns off "alarm flag"
  settings = settings - B00000100;

  // set control register 2 to desired value  
  Wire.beginTransmission(PCF_8563_ADDRESS);
  Wire.write(PCF_8563_REG2_ADDR);          // index to cntrl reg 2
  Wire.write(settings);            // set value of PCF cntrl reg 2
  Wire.endTransmission();
}

/*************************************************************
 * PCF8563 Timer: 
 * If an alarm has been tripped reset alarm
 *************************************************************/
void checkPCF8563Alarm()
{
  byte settings = readPCF8563CntrlReg2();
  
  // if alarm is set, reset "alarm flag"
  if ((settings & B00000100) == B00000100)
  {
    PCF8563AlarmOff();
  }
}

/************************************************************************************** 
 * PCF8563 Timer:
 * Configure timer to sec/min/hr, set timer value, and kick off initial alarm processing 
 **************************************************************************************/
void setTimerPCF8563()
{
  // set clock rate of 1 Hz, timer configured for minutes (vs seconds)
  Wire.beginTransmission(PCF_8563_ADDRESS);
  Wire.write(PCF_8563_TIMER_CNTL_ADDR);      // access PCF timer control
  Wire.write(PCF_8563_ENABLE_TIMER_MINS);    // timer_setting_1
  Wire.endTransmission();
 
  Wire.beginTransmission(PCF_8563_ADDRESS);
  Wire.write(PCF_8563_TIMER_VAL_ADDR);       // access PCF timer value
  Wire.write(PCF_8563_SET_TIMER_60_MIN);     // timer_setting_2
  Wire.endTransmission();

  // This section forces an immediate alarm trigger / wake up processing to
  // occur (instead of waiting for set period of time) after initial boot up
  // of the microcontroller.  All subsequent alarm trigger / wake up processing
  // will occur after the set period of time has elapsed.  Remove this section
  // if you don't want alarm trigger / wake  up processing immediately following
  // boot up of the microcontroller.
  Wire.beginTransmission(PCF_8563_ADDRESS);   // access PCF
  Wire.write(PCF_8563_REG2_ADDR);             // index to PCF cntrl reg 2
  Wire.write(B00000001);                      // clear all flags, enable timer intterupt
  Wire.endTransmission();	
} 

/*************************************************************************
 * Comms:
 * Send sensor data to Internet endpoint using ESP8266 AT commands.  
 *************************************************************************/
void sendSensorData(char* strTemp, char* strHumidity, char* strSensor1, 
                    char* strSensor2, char* strSensor3, char* strVcc )
{
  char buffer[256];
  
  // configure details of the Internet endpoint
  sprintf(buffer,"%s%s%s", "AT+CIPSTART=\"TCP\",\"", THING_SPEAK_IP, "\",80");
  
  // connect to Internet endpoint
  Serial.println(buffer); 
  delay(1000);
     
  buffer[0] = "\0";
#ifdef USING_DHT
  sprintf(buffer,"%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s", GET_COMMAND, TS_API_KEY,
          "&field1=", VINEYARD_SENSOR_STATION_LOCATION, "&field2=", strTemp, 
		  "&field3=", strHumidity, "&field4=", strVcc, "&field5=", strSensor1,
		  "&field6=", strSensor2, "&field7=", strSensor3,"\r\n");  
#else
  sprintf(buffer,"%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s", GET_COMMAND, TS_API_KEY,
          "&field1=", VINEYARD_SENSOR_STATION_LOCATION, "&field2=", strTemp, 
		  "&field3=", strVcc, "&field4=", strSensor1, "&field5=", strSensor2,
		  "&field6=", strSensor3, "\r\n");  
#endif		  

  // send sensor readings to Internet endpoint
  Serial.print("AT+CIPSEND=");     
  Serial.println(strlen(buffer));   
  delay(1000);
  Serial.println(buffer); 
  delay(5000); 
}

/*************************************************************************
 * Comms:
 * Turn on the ESP8266 and then join the local wireless network
 *
 * ESP8266
 *   You configure the ESP8266 to work in "station", "access point" mode,
 *   or "station + access point" mode.  "station" mode enables the ESP8266 
 *   to connect to a WiFi netowrk while "access point" mode enables the 
 *   ESP8266 to create its own network allowing other devices to connect to it.  
 *************************************************************************/
void joinWirelessNetwork()
{  
  char buffer[256];
  
  // turn on WiFi chip
  digitalWrite(WIFI_PIN, HIGH);
  delay(1000);  
   
  Serial.println("AT+RST");            // restart the ESP8266
  delay (1000); 
  
  Serial.println("AT+CWMODE_DEF=1");   // "station" mode saved to firmware
  delay(1000);
  
  sprintf(buffer,"%s%s%s%s%s", "AT+CWJAP_DEF=\"", SSID, "\",\"", PASS, "\"");
  Serial.println(buffer);
  delay(5000);
}
