# pg-table-subscriber

### Usage
```javascript
    const { start, beforeUpdate } = require('pg-table-subscriber');

    start(dbConfig);

    beforeUpdate('some_table', ({ previous, record }) => {
        console.info('We`ve got something updated!');
        console.info('It was like this:', previous);
        console.info('A became:', record);
        console.info('Well, okay...');
    });
```
