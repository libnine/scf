<template>
  <div id="app">
    <router-view/>
  </div>
</template>

<script>
export default {
  name: 'App',
};
</script>

<style>
#app {
  font-family: 'Avenir', Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  color: #2c3e50;
  margin-top: 60px;
}

#dataoutput {
  transition: color 10s;
}

#dataoutput.fade-out {
  color: transparent;
}

label {
  float: center;
  width: 10em;
  margin-right: 1px;
}
div {
  align-content: center;
  align-items: center;
  align-self: center;
  justify-content: center;
  justify-items: auto;
}

button {
  width: 40%;
}

h1 {
  font-family: 'Avenir', Helvetica, Arial, sans-serif;
  font-weight: lighter;
  font-size: 35px;
  padding: 1%;
  color:rgba(25, 25, 182, 0.479);
}

h5 {
  font-family: 'Avenir', Helvetica, Arial, sans-serif;
  font-weight: lighter;
  font-size: 35px;
  padding: 1%;
  color:rgba(6, 29, 51, 0.685); 
}

select {
  width: 100em;
}

.grid-container {
  text-align: center;
  align-content: center;
  align-items: center;
  display: grid;
}

.form-group {
  align-content: center;
  border: 0.5px dotted rgba(25, 25, 182, 0.096);
  padding: 10px;
  width: 400px;
  height: 125px;
}

.form-group input::placeholder {
  align-items: center;
  color: rgba(4, 8, 66, 0.233);
  margin: auto;
}

.btn {
  margin-top: 3%;
}

.form-control {
  width: 100%;
  margin: 5%;
}

.form-control:hover {
  box-shadow: 2px 2px;
}

.lab {
  color:rgba(5, 5, 61, 0.664);
  text-align: center;
}

</style>
