import {Schema, model} from "mongoose"

const userSchema = new Schema({
    name:{
        type:String,
        required:[true,'name is required'],
        trim:true
    },
    email:{
        type:String,
        required:[true,'email is required'],
        unique:true,
        lowercase: true,
        trim: true,
        validate: {
            validator: function(value) {
                // Standard regex pattern for validating an email format
                const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+\$/;
                return emailRegex.test(value);
            },
            // {VALUE} dynamically injects the invalid email string into the error message
            message: props => `${props.value} is not a valid email address!`
        }
    },
    role: {
        type: String,
        enum: ['user', 'admin', 'moderator'],
        default: 'user'
    },
    isActive: {
        type: Boolean,
        default: true
    }
})

const User = model('User',userSchema)
export default User
