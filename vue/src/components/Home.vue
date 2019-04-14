<template>
  <div class="grid-container">
    <h1 id="message">{{ message }}</h1>
    <form class="form-inline">
      <div class="form-group">  
        <label for="sport_name" class="lab">Sport</label>
        <select name="sport_name" id="sport_name" class="form-control" style="width: 250px" v-model="sport_name">
          <option v-for="sport in possible_sports" :key="sport">{{ sport }}</option>
        </select>
      </div>
      <div class="form-group">
        <label for="year" class="lab">Year</label>
        <input type="number" class="form-control" placeholder="1978" id="year" size="20" v-model="year">
      </div>
      <div class="form-group">
        <label for="set_name" class="lab">Set Name</label>
        <input type="text" class="form-control" placeholder="Topps" id="set_name" size="35" v-model="set_name">
      </div>
    </form>
    <div class="container">
      <button class="btn btn-primary" @click="flask()">Submit</button>
    </div>
    <br>
    <div class="container">
      <h5 id="dataoutput" :class="{'fade-out': fadeOut}">{{ resp_msg }}</h5>
    </div>
  </div>
</template>

<script>
import axios from 'axios'

export default {
  data () {
    return {
      fadeOut: false,
      path: '',
      resp_msg: null,
      set_name: null,
      sport_name: null,
      year: null,
      possible_sports: ['Baseball', 'Basketball', 'Football', 'Hockey', 'Boxing', 'Non-Sport']
    }
  },
  methods: {
    flask() {
      axios.post(this.path, 
      {
        year: this.year,
        category: this.possible_sports.indexOf(this.sport_name) + 1,
        set_name: this.set_name
      }, 
      {timeout: 1000 * 60,
      headers: {"Access-Control-Allow-Origin": "*"}
      })
      .then((response) => {
        console.log(response.data)
        this.resp_msg = response.data[0]
      }),
      this.fadeOut = true,
      setTimeout(() => {
        this.fadeOut = false,
        this.resp_msg = null
      }, 10000)
    },

    hrs() {
      const hours = new Date().getHours();
      let msg = '';

      switch (true) {
        case (hours > 0 && hours < 12):
          return msg = 'Good morning!';

        case (hours >= 12 && hours < 17):
          return msg = 'Good afternoon!';

        case hours >= 17 && hours < 24:
          return msg = 'Good evening!';

        default:
          return msg = 'Hi!';
      }
    },

    getHrs() {
      const d = new Date();
      const day = new Array(7);
      day[0] = 'Sunday';
      day[1] = 'Monday';
      day[2] = 'Tuesday';
      day[3] = 'Wednesday';
      day[4] = 'Thursday';
      day[5] = 'Friday';
      day[6] = 'Saturday';

      this.message = this.hrs() + ' Happy ' + day[d.getDay()] + '!';
    },
  },

  created() {
    this.getHrs()
    this.sport_name = this.possible_sports[0]
  },
};
</script>

